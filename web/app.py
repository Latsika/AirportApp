# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import sqlite3
import sys
import time
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from email.message import EmailMessage
from io import BytesIO, StringIO
from functools import wraps
from secrets import token_urlsafe
from typing import Optional

import smtplib
import csv
from flask import (
    Flask,
    abort,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from database.db import (  # noqa: E402
    delete_app_state,
    ensure_default_admin,
    get_app_state,
    get_connection,
    init_db,
    log_auth_event,
    log_sales_event,
    set_app_state,
)
from utils.security import hash_password, verify_password_and_upgrade  # noqa: E402
from reportlab.lib.pagesizes import letter, A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.textlabels import Label

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-please-change")
app.config["SESSION_PERMANENT"] = True
app.permanent_session_lifetime = timedelta(minutes=30)

init_db()
ensure_default_admin(hash_password)

PASSWORD_RE = re.compile(r"^(?=.*[A-Za-z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,}$")
APPROVER_ROLES = {"Admin", "Deputy"}


def _client_ip() -> Optional[str]:
    return request.headers.get("X-Forwarded-For", request.remote_addr)


def _user_agent() -> Optional[str]:
    return request.headers.get("User-Agent")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_utc_date() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _month_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _month_date_range(year: int, month: int) -> tuple[str, str]:
    month = min(12, max(1, month))
    start_date = datetime(year, month, 1, tzinfo=timezone.utc).date()
    if month == 12:
        end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc).date() - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc).date() - timedelta(days=1)
    return start_date.isoformat(), end_date.isoformat()


def _compute_monthly_airport_total(year: int, month: int) -> float:
    start_date, end_date = _month_date_range(year, month)
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT SUM(si.total_amount) AS total
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            WHERE si.fee_source = 'airport'
              AND date(s.sold_at_utc) BETWEEN ? AND ?
            """,
            (start_date, end_date),
        )
        row = cur.fetchone()
    return float(row["total"] or 0)

def _report_rows_by_airline(conn, date_filter: str, is_month: bool, source: str):
    cur = conn.cursor()
    if is_month:
        cur.execute(
            """
            SELECT a.id, a.name, a.code,
                   d.dest_name AS destination_name,
                   d.dest_code AS destination_code,
                   CASE
                       WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_key, si.fee_key)
                       WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_key, si.fee_key)
                       ELSE COALESCE(si.fee_key, '')
                   END AS fee_key,
                   CASE
                       WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_name, si.fee_name, si.fee_key)
                       WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                       ELSE COALESCE(si.fee_name, si.fee_key)
                   END AS fee_name,
                   SUM(si.quantity) AS qty, SUM(si.total_amount) AS total,
                   SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
                   SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            JOIN airlines a ON a.id = s.airline_id
            LEFT JOIN airline_destinations d ON d.id = s.destination_id
            LEFT JOIN airline_fees af ON af.id = si.fee_id AND si.fee_source = 'airline'
            LEFT JOIN airport_service_fees apf ON apf.id = si.fee_id AND si.fee_source = 'airport'
            WHERE si.fee_source = ? AND substr(s.sold_at_utc, 1, 7) = ?
            GROUP BY a.id, d.id, 6, 7
            ORDER BY a.name COLLATE NOCASE ASC, d.dest_name COLLATE NOCASE ASC, 7 COLLATE NOCASE ASC
            """,
            (source, date_filter),
        )
    else:
        cur.execute(
            """
            SELECT a.id, a.name, a.code,
                   d.dest_name AS destination_name,
                   d.dest_code AS destination_code,
                   CASE
                       WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_key, si.fee_key)
                       WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_key, si.fee_key)
                       ELSE COALESCE(si.fee_key, '')
                   END AS fee_key,
                   CASE
                       WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_name, si.fee_name, si.fee_key)
                       WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                       ELSE COALESCE(si.fee_name, si.fee_key)
                   END AS fee_name,
                   SUM(si.quantity) AS qty, SUM(si.total_amount) AS total,
                   SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
                   SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            JOIN airlines a ON a.id = s.airline_id
            LEFT JOIN airline_destinations d ON d.id = s.destination_id
            LEFT JOIN airline_fees af ON af.id = si.fee_id AND si.fee_source = 'airline'
            LEFT JOIN airport_service_fees apf ON apf.id = si.fee_id AND si.fee_source = 'airport'
            WHERE si.fee_source = ? AND date(s.sold_at_utc) = ?
            GROUP BY a.id, d.id, 6, 7
            ORDER BY a.name COLLATE NOCASE ASC, d.dest_name COLLATE NOCASE ASC, 7 COLLATE NOCASE ASC
            """,
            (source, date_filter),
        )
    return cur.fetchall()


def _report_totals_by_airline(conn, date_filter: str, is_month: bool, source: str):
    cur = conn.cursor()
    if is_month:
        cur.execute(
            """
            SELECT a.id, a.name, a.code, SUM(si.total_amount) AS total,
                   SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
                   SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            JOIN airlines a ON a.id = s.airline_id
            WHERE si.fee_source = ? AND substr(s.sold_at_utc, 1, 7) = ?
            GROUP BY a.id
            ORDER BY a.name COLLATE NOCASE ASC
            """,
            (source, date_filter),
        )
    else:
        cur.execute(
            """
            SELECT a.id, a.name, a.code, SUM(si.total_amount) AS total,
                   SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
                   SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            JOIN airlines a ON a.id = s.airline_id
            WHERE si.fee_source = ? AND date(s.sold_at_utc) = ?
            GROUP BY a.id
            ORDER BY a.name COLLATE NOCASE ASC
            """,
            (source, date_filter),
        )
    return cur.fetchall()


def _report_total_all(conn, date_filter: str, is_month: bool, source: str):
    cur = conn.cursor()
    if is_month:
        cur.execute(
            """
            SELECT SUM(si.total_amount) AS total,
                   SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
                   SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            WHERE si.fee_source = ? AND substr(s.sold_at_utc, 1, 7) = ?
            """,
            (source, date_filter),
        )
    else:
        cur.execute(
            """
            SELECT SUM(si.total_amount) AS total,
                   SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
                   SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
            FROM sale_items si
            JOIN sales s ON s.id = si.sale_id
            WHERE si.fee_source = ? AND date(s.sold_at_utc) = ?
            """,
            (source, date_filter),
        )
    row = cur.fetchone()
    return {
        "total": float(row["total"] or 0),
        "cash_total": float(row["cash_total"] or 0),
        "card_total": float(row["card_total"] or 0),
    }


def _load_custom_report_filters():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, code FROM airlines ORDER BY name COLLATE NOCASE ASC")
        airlines = cur.fetchall()
        cur.execute(
            "SELECT id, fee_key, fee_name FROM airline_fees ORDER BY fee_name COLLATE NOCASE ASC"
        )
        airline_items = cur.fetchall()
        cur.execute(
            "SELECT id, fee_key, fee_name FROM airport_service_fees ORDER BY fee_name COLLATE NOCASE ASC"
        )
        airport_items = cur.fetchall()
        cur.execute(
            "SELECT id, fullname, nickname FROM users ORDER BY fullname COLLATE NOCASE ASC"
        )
        sellers = cur.fetchall()
        cur.execute(
            """
            SELECT id, airline_id, dest_code, dest_name, active
            FROM airline_destinations
            ORDER BY dest_name COLLATE NOCASE ASC
            """
        )
        destinations = cur.fetchall()
    return airlines, airline_items, airport_items, sellers, destinations


def _parse_custom_report_filters(args):
    date_from = _sanitize(args.get("date_from")) or _today_utc_date()
    date_to = _sanitize(args.get("date_to")) or date_from
    date_from, date_to = _normalize_date_range(date_from, date_to)

    selected_airlines = args.getlist("airline_id")
    selected_destinations = args.getlist("destination_id")
    selected_items = args.getlist("item_id")
    selected_payments = args.getlist("payment_method")
    selected_sellers = args.getlist("sold_by")
    selected_sources = args.getlist("source")

    airline_item_ids = []
    airport_item_ids = []
    include_ticket = False
    ticket_airline_ids: list[int] = []
    for v in selected_items:
        if v == "ticket":
            include_ticket = True
        elif v.startswith("airline:"):
            try:
                airline_item_ids.append(int(v.split(":", 1)[1]))
            except ValueError:
                continue
        elif v.startswith("ticket:"):
            try:
                ticket_airline_ids.append(int(v.split(":", 1)[1]))
                include_ticket = True
            except ValueError:
                continue
        elif v.startswith("airport:"):
            try:
                airport_item_ids.append(int(v.split(":", 1)[1]))
            except ValueError:
                continue

    airline_ids = [int(x) for x in selected_airlines if x.isdigit()]
    destination_ids = [int(x) for x in selected_destinations if x.isdigit()]
    payment_methods = [x for x in selected_payments if x in {"CASH", "CARD"}]
    sold_by_ids = [int(x) for x in selected_sellers if x.isdigit()]

    include_airport = "airport" in selected_sources or "airport" in selected_airlines or bool(airport_item_ids)
    include_airline = (
        "airline" in selected_sources
        or bool(airline_ids)
        or bool(airline_item_ids)
        or include_ticket
    )

    filters = {
        "date_from": date_from,
        "date_to": date_to,
        "airline_ids": airline_ids,
        "destination_ids": destination_ids,
        "payment_methods": payment_methods,
        "sold_by_ids": sold_by_ids,
        "airline_item_ids": airline_item_ids,
        "airport_item_ids": airport_item_ids,
        "include_ticket": include_ticket,
        "ticket_airline_ids": ticket_airline_ids,
        "include_airport": include_airport,
        "include_airline": include_airline,
    }
    return filters, {
        "date_from": date_from,
        "date_to": date_to,
        "selected_airlines": selected_airlines,
        "selected_destinations": selected_destinations,
        "selected_items": selected_items,
        "selected_payments": selected_payments,
        "selected_sellers": selected_sellers,
        "selected_sources": selected_sources,
    }


def _normalize_date_range(date_from: str, date_to: str) -> tuple[str, str]:
    def _parse_date(value: str):
        value = (value or "").strip()
        value = re.sub(r"\s+", "", value)
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            return None

    start_date = _parse_date(date_from) or datetime.now(timezone.utc).date()
    end_date = _parse_date(date_to) or start_date
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    return start_date.isoformat(), end_date.isoformat()


def _custom_report_where(filters: dict):
    params = []
    where = ["date(s.sold_at_utc) BETWEEN ? AND ?"]
    params.extend([filters["date_from"], filters["date_to"]])

    if filters["airline_ids"]:
        placeholders = ",".join(["?"] * len(filters["airline_ids"]))
        where.append(f"s.airline_id IN ({placeholders})")
        params.extend(filters["airline_ids"])

    if filters.get("destination_ids"):
        placeholders = ",".join(["?"] * len(filters["destination_ids"]))
        where.append(f"s.destination_id IN ({placeholders})")
        params.extend(filters["destination_ids"])

    if filters["payment_methods"]:
        placeholders = ",".join(["?"] * len(filters["payment_methods"]))
        where.append(f"s.payment_method IN ({placeholders})")
        params.extend(filters["payment_methods"])

    if filters["sold_by_ids"]:
        placeholders = ",".join(["?"] * len(filters["sold_by_ids"]))
        where.append(f"s.created_by IN ({placeholders})")
        params.extend(filters["sold_by_ids"])

    item_conditions = []
    item_params = []
    sources = []
    if filters["include_airline"]:
        sources.append("airline")
    if filters["include_airport"]:
        sources.append("airport")
    if filters["include_ticket"]:
        sources.append("ticket")
    if filters["airline_item_ids"]:
        placeholders = ",".join(["?"] * len(filters["airline_item_ids"]))
        item_conditions.append(f"(si.fee_source = 'airline' AND si.fee_id IN ({placeholders}))")
        item_params.extend(filters["airline_item_ids"])
    if filters["airport_item_ids"]:
        placeholders = ",".join(["?"] * len(filters["airport_item_ids"]))
        item_conditions.append(f"(si.fee_source = 'airport' AND si.fee_id IN ({placeholders}))")
        item_params.extend(filters["airport_item_ids"])
    if filters["include_ticket"]:
        if filters.get("ticket_airline_ids"):
            placeholders = ",".join(["?"] * len(filters["ticket_airline_ids"]))
            item_conditions.append(
                f"(si.fee_source = 'ticket' AND s.airline_id IN ({placeholders}))"
            )
            item_params.extend(filters["ticket_airline_ids"])
        else:
            item_conditions.append("(si.fee_source = 'ticket')")

    if sources:
        placeholders = ",".join(["?"] * len(sources))
        where.append(f"si.fee_source IN ({placeholders})")
        params.extend(sources)

    if item_conditions:
        where.append("(" + " OR ".join(item_conditions) + ")")
        params.extend(item_params)
    elif not sources:
        return None, None

    return where, params


def _build_custom_report(filters: dict):
    where, params = _custom_report_where(filters)
    if where is None:
        return [], {"dates": [], "series": []}

    sql = f"""
        SELECT
            s.id AS sale_id,
            a.name AS airline_name,
            a.code AS airline_code,
            d.dest_name AS destination_name,
            d.dest_code AS destination_code,
            s.sold_at_utc,
            s.payment_method,
            u.fullname AS sold_by_name,
            u.nickname AS sold_by_nick,
            si.fee_source,
            CASE
                WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_key, si.fee_key)
                WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_key, si.fee_key)
                ELSE COALESCE(si.fee_key, '')
            END AS fee_key,
            CASE
                WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_name, si.fee_name, si.fee_key)
                WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                ELSE COALESCE(si.fee_name, si.fee_key)
            END AS fee_name,
            si.quantity,
            si.total_amount
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN airlines a ON a.id = s.airline_id
        LEFT JOIN airline_destinations d ON d.id = s.destination_id
        LEFT JOIN users u ON u.id = s.created_by
        LEFT JOIN airline_fees af ON af.id = si.fee_id AND si.fee_source = 'airline'
        LEFT JOIN airport_service_fees apf ON apf.id = si.fee_id AND si.fee_source = 'airport'
        WHERE {" AND ".join(where)}
        ORDER BY s.sold_at_utc DESC, a.name COLLATE NOCASE ASC, d.dest_name COLLATE NOCASE ASC,
                 fee_name COLLATE NOCASE ASC
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    # build date series for chart (Y=quantity, X=date)
    try:
        start_date = datetime.fromisoformat(filters["date_from"]).date()
        end_date = datetime.fromisoformat(filters["date_to"]).date()
    except ValueError:
        start_date = datetime.now(timezone.utc).date()
        end_date = start_date
    if end_date < start_date:
        start_date, end_date = end_date, start_date

    date_list = []
    d = start_date
    while d <= end_date:
        date_list.append(d.isoformat())
        d += timedelta(days=1)

    def _destination_label(row):
        name = (row["destination_name"] or "").strip()
        code = (row["destination_code"] or "").strip()
        if name and code:
            return f"{name} ({code})"
        if name:
            return name
        if code:
            return code
        return ""

    series = {}
    for r in rows:
        date_key = (r["sold_at_utc"] or "")[:10]
        if not date_key:
            continue
        dest_label = _destination_label(r)
        if r["fee_source"] == "airport":
            series_key = f"Airport - {r['fee_key']}" if r["fee_key"] else "Airport"
        elif r["fee_source"] == "ticket":
            if r["airline_name"]:
                if r["airline_code"]:
                    series_key = f"{r['airline_name']} ({r['airline_code']}) Plane Ticket"
                else:
                    series_key = f"{r['airline_name']} Plane Ticket"
            else:
                series_key = "Plane Ticket"
        elif filters["airline_ids"] and r["fee_key"]:
            series_key = f"{r['airline_code'] or r['airline_name']} - {r['fee_key']}"
        elif r["fee_key"]:
            series_key = r["fee_key"]
        else:
            series_key = r["fee_name"] or "Item"
        if dest_label and filters.get("destination_ids"):
            series_key = f"{series_key} @ {dest_label}"
        if series_key not in series:
            series[series_key] = {k: 0 for k in date_list}
        series[series_key][date_key] = series[series_key].get(date_key, 0) + int(r["quantity"] or 0)

    series_list = []
    for k, v in series.items():
        series_list.append({"label": k, "values": [v.get(d, 0) for d in date_list]})

    chart_payload = {"dates": date_list, "series": series_list}
    return rows, chart_payload


def _custom_report_items_by_source(filters: dict, source: str):
    where, params = _custom_report_where(filters)
    if where is None:
        return []

    where = list(where)
    params = list(params)
    if source == "airline" and filters["include_ticket"]:
        where.append("(si.fee_source = 'airline' OR si.fee_source = 'ticket')")
    else:
        where.append("si.fee_source = ?")
        params.append(source)

    sql = f"""
        SELECT a.id, a.name, a.code,
               d.dest_name AS destination_name,
               d.dest_code AS destination_code,
               CASE
                   WHEN si.fee_source = 'ticket' THEN 'TICKET'
                   WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_key, si.fee_key)
                   WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_key, si.fee_key)
                   ELSE COALESCE(si.fee_key, '')
               END AS fee_key,
               CASE
                   WHEN si.fee_source = 'ticket' THEN COALESCE(si.fee_name, 'Plane Ticket')
                   WHEN si.fee_source = 'airline' THEN COALESCE(af.fee_name, si.fee_name, si.fee_key)
                   WHEN si.fee_source = 'airport' THEN COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                   ELSE COALESCE(si.fee_name, si.fee_key)
               END AS fee_name,
               SUM(si.quantity) AS qty, SUM(si.total_amount) AS total,
               SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
               SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN airlines a ON a.id = s.airline_id
        LEFT JOIN airline_destinations d ON d.id = s.destination_id
        LEFT JOIN airline_fees af ON af.id = si.fee_id AND si.fee_source = 'airline'
        LEFT JOIN airport_service_fees apf ON apf.id = si.fee_id AND si.fee_source = 'airport'
        WHERE {" AND ".join(where)}
        GROUP BY a.id, d.id, 6, 7
        ORDER BY a.name COLLATE NOCASE ASC, d.dest_name COLLATE NOCASE ASC,
                 7 COLLATE NOCASE ASC
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def _custom_report_totals_by_airline(filters: dict, source: str):
    where, params = _custom_report_where(filters)
    if where is None:
        return []

    where = list(where)
    params = list(params)
    if source == "airline" and filters["include_ticket"]:
        where.append("(si.fee_source = 'airline' OR si.fee_source = 'ticket')")
    else:
        where.append("si.fee_source = ?")
        params.append(source)

    sql = f"""
        SELECT a.id, a.name, a.code, SUM(si.total_amount) AS total,
               SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
               SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        JOIN airlines a ON a.id = s.airline_id
        WHERE {" AND ".join(where)}
        GROUP BY a.id
        ORDER BY a.name COLLATE NOCASE ASC
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall()


def _custom_report_total_all(filters: dict, source: str):
    where, params = _custom_report_where(filters)
    if where is None:
        return {"total": 0.0, "cash_total": 0.0, "card_total": 0.0}

    where = list(where)
    params = list(params)
    if source == "airline" and filters["include_ticket"]:
        where.append("(si.fee_source = 'airline' OR si.fee_source = 'ticket')")
    else:
        where.append("si.fee_source = ?")
        params.append(source)

    sql = f"""
        SELECT SUM(si.total_amount) AS total,
               SUM(CASE WHEN s.payment_method = 'CASH' THEN si.total_amount ELSE 0 END) AS cash_total,
               SUM(CASE WHEN s.payment_method = 'CARD' THEN si.total_amount ELSE 0 END) AS card_total
        FROM sale_items si
        JOIN sales s ON s.id = si.sale_id
        WHERE {" AND ".join(where)}
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
    return {
        "total": float(row["total"] or 0),
        "cash_total": float(row["cash_total"] or 0),
        "card_total": float(row["card_total"] or 0),
    }


def _custom_report_to_pdf(title: str, rows, chart_data, date_from: str, date_to: str):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=36,
        rightMargin=36,
        topMargin=30,
        bottomMargin=30,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontSize=16,
        leading=20,
        spaceAfter=12,
        textColor=colors.black,
    )
    section_style = ParagraphStyle(
        "SectionTitle",
        parent=styles["Heading2"],
        fontSize=12,
        leading=14,
        spaceBefore=6,
        spaceAfter=6,
        textColor=colors.black,
    )
    normal_style = ParagraphStyle(
        "NormalCell", parent=styles["BodyText"], fontSize=9, leading=11, textColor=colors.black
    )

    def wrap_table_data(data):
        wrapped = []
        for row in data:
            wrapped.append([Paragraph(str(cell), normal_style) for cell in row])
        return wrapped

    def make_table(data, col_widths, total_row=False):
        t = Table(wrap_table_data(data), colWidths=col_widths)
        style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ]
        )
        if total_row:
            style.add("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc"))
            style.add("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold")
            style.add("FONTSIZE", (0, 1), (-1, 1), 12)
        t.setStyle(style)
        return t

    elements = [Paragraph(title, title_style)]

    # parse rows to sections + tables
    sections = []
    i = 0
    while i < len(rows):
        row = rows[i]
        if not row:
            i += 1
            continue
        if len(row) == 1 and isinstance(row[0], str):
            heading = row[0]
            table_rows = []
            i += 1
            while i < len(rows):
                r2 = rows[i]
                if not r2:
                    break
                if len(r2) == 1 and isinstance(r2[0], str):
                    break
                table_rows.append(r2)
                i += 1
            sections.append((heading, table_rows))
            continue
        i += 1

    page_width = doc.width
    for heading, table_rows in sections:
        header = Table([[Paragraph(heading, section_style)]], colWidths=[doc.width])
        header.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f1f5f9")),
                    ("BOX", (0, 0), (-1, -1), 1, colors.HexColor("#cbd5e1")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        elements.append(header)
        if not table_rows:
            elements.append(Spacer(1, 6))
            continue

        header_row = table_rows[0]
        data_rows = table_rows[1:]
        if header_row == ["Airline", "Destination", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"]:
            col_widths = [
                page_width * 0.16,
                page_width * 0.18,
                page_width * 0.12,
                page_width * 0.22,
                page_width * 0.06,
                page_width * 0.10,
                page_width * 0.08,
                page_width * 0.08,
            ]
            elements.append(make_table([header_row] + data_rows, col_widths))
        elif header_row == ["Airline", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"]:
            col_widths = [
                page_width * 0.18,
                page_width * 0.14,
                page_width * 0.26,
                page_width * 0.08,
                page_width * 0.12,
                page_width * 0.11,
                page_width * 0.11,
            ]
            elements.append(make_table([header_row] + data_rows, col_widths))
        elif header_row == ["Airline", "Total", "Cash", "Card"]:
            col_widths = [
                page_width * 0.46,
                page_width * 0.18,
                page_width * 0.18,
                page_width * 0.18,
            ]
            elements.append(make_table([header_row] + data_rows, col_widths))
        elif header_row == ["Total", "Cash", "Card"] and len(data_rows) == 1:
            col_widths = [page_width * 0.34, page_width * 0.33, page_width * 0.33]
            elements.append(make_table([header_row] + data_rows, col_widths, total_row=True))
        else:
            col_count = max(len(r) for r in table_rows)
            col_widths = [page_width / col_count] * col_count
            elements.append(make_table(table_rows, col_widths))
        elements.append(Spacer(1, 10))

    if chart_data and chart_data.get("series"):
        elements.append(PageBreak())
        elements.append(Paragraph(f"Chart ({date_from} to {date_to})", section_style))
        from reportlab.graphics.charts.lineplots import LinePlot

        drawing = Drawing(doc.width, 360)
        chart = LinePlot()
        chart.x = 40
        chart.y = 40
        chart.height = 260
        chart.width = doc.width - 80

        dates = chart_data.get("dates", [])
        series = chart_data.get("series", [])[:6]
        chart.data = [
            [(i, v) for i, v in enumerate(s["values"])]
            for s in series
        ]
        max_val = max((v for s in series for v in s["values"]), default=0)
        chart.yValueAxis.valueMin = 0
        chart.yValueAxis.valueMax = max_val * 1.2 if max_val else 1
        chart.yValueAxis.valueStep = max(1, int(chart.yValueAxis.valueMax / 5))
        chart.xValueAxis.valueMin = 0
        chart.xValueAxis.valueMax = max(1, len(dates) - 1)
        chart.xValueAxis.valueSteps = list(range(len(dates)))
        chart.xValueAxis.labelTextFormat = lambda v: dates[int(v)] if int(v) < len(dates) else ""

        colors_list = [
            colors.HexColor("#0ea5e9"),
            colors.HexColor("#10b981"),
            colors.HexColor("#f59e0b"),
            colors.HexColor("#ef4444"),
            colors.HexColor("#8b5cf6"),
            colors.HexColor("#14b8a6"),
        ]
        for idx, _ in enumerate(series):
            chart.lines[idx].strokeColor = colors_list[idx % len(colors_list)]
            chart.lines[idx].strokeWidth = 2

        drawing.add(chart)
        label = Label()
        label.setOrigin(40, 320)
        label.setText("Quantity by date (up to 6 series)")
        label.fontSize = 9
        drawing.add(label)
        elements.append(drawing)

        legend_rows = [["Series", "Color"]]
        for idx, s in enumerate(series):
            legend_rows.append([s["label"], ""])
        legend = Table(legend_rows, colWidths=[doc.width * 0.7, doc.width * 0.3])
        legend.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        for idx in range(1, len(legend_rows)):
            legend.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (1, idx), (1, idx), colors_list[(idx - 1) % len(colors_list)]),
                    ]
                )
            )
        elements.append(legend)

    doc.build(elements)
    return buffer.getvalue()


def _build_report_payload(date_filter: str, is_month: bool):
    with get_connection() as conn:
        airline_items = _report_rows_by_airline(conn, date_filter, is_month, "airline")
        airport_items = _report_rows_by_airline(conn, date_filter, is_month, "airport")
        airline_totals = _report_totals_by_airline(conn, date_filter, is_month, "airline")
        airport_totals = _report_totals_by_airline(conn, date_filter, is_month, "airport")
        airline_all = _report_total_all(conn, date_filter, is_month, "airline")
        airport_all = _report_total_all(conn, date_filter, is_month, "airport")
        combined = {
            "total": airline_all["total"] + airport_all["total"],
            "cash_total": airline_all["cash_total"] + airport_all["cash_total"],
            "card_total": airline_all["card_total"] + airport_all["card_total"],
        }
    return {
        "airline_items": airline_items,
        "airport_items": airport_items,
        "airline_totals": airline_totals,
        "airport_totals": airport_totals,
        "airline_all": airline_all,
        "airport_all": airport_all,
        "combined_all": combined,
    }


def _redirect_after_login(role: str | None):
    if role in {"Admin", "Deputy"}:
        return redirect(url_for("admin_hub"))
    return redirect(url_for("user_hub"))


def _generate_temp_password() -> str:
    while True:
        candidate = token_urlsafe(9)
        if PASSWORD_RE.match(candidate):
            return candidate


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return wrapper


def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not (session.get("logged_in") and session.get("role") == "Admin"):
            if session.get("logged_in"):
                return redirect(url_for("user_hub"))
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return wrapper


def approver_required(f):
    """Admin or Deputy can approve pending accounts."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not (session.get("logged_in") and session.get("role") in APPROVER_ROLES):
            flash("Approver privileges required.")
            return redirect(url_for("index"))
        return f(*args, **kwargs)

    return wrapper


def require_csrf() -> None:
    token = request.form.get("csrf_token")
    if not token or token != session.get("csrf_token"):
        abort(400)


def _sanitize(value: str) -> str:
    return (value or "").strip()


def _is_valid_email(value: str) -> bool:
    if not value or "@" not in value:
        return False
    local, _, domain = value.rpartition("@")
    if not local or not domain or "." not in domain:
        return False
    return True


DEFAULT_NOTIFICATION_TEMPLATES = [
    {
        "slug": "new_user_created",
        "name": "New user created (waiting approval)",
        "subject": "New user created - {UserName} waiting for approval",
        "body": "New user created - {UserName} waiting for approval.",
    },
    {
        "slug": "daily_report_created",
        "name": "Daily report created",
        "subject": "Daily report created by {UserName} ({ReportDate})",
        "body": "Daily report created by {UserName} for {ReportDate}.",
    },
    {
        "slug": "monthly_report_created",
        "name": "Monthly report created",
        "subject": "Monthly report created by {UserName} ({ReportMonth})",
        "body": "Monthly report created by {UserName} for {ReportMonth}.",
    },
    {
        "slug": "daily_report_not_created",
        "name": "Daily report NOT created",
        "subject": "Daily report NOT created ({ReportDate})",
        "body": "Daily report was not created for {ReportDate}.",
    },
    {
        "slug": "monthly_report_not_created",
        "name": "Monthly report NOT created",
        "subject": "Monthly report NOT created ({ReportMonth})",
        "body": "Monthly report was not created for {ReportMonth}.",
    },
    {
        "slug": "user_deleted",
        "name": "User deleted",
        "subject": "User deleted - {UserName}",
        "body": "User deleted - {UserName}.",
    },
]


def _ensure_default_notification_templates() -> None:
    now = _utc_now_iso()
    with get_connection() as conn:
        cur = conn.cursor()
        for t in DEFAULT_NOTIFICATION_TEMPLATES:
            cur.execute("SELECT 1 FROM notification_templates WHERE slug = ?", (t["slug"],))
            if cur.fetchone():
                continue
            cur.execute(
                """
                INSERT INTO notification_templates (name, slug, subject, body, enabled, created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                """,
                (t["name"], t["slug"], t["subject"], t["body"], now, now),
            )
        conn.commit()


def _slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    return value or "notification"


def _render_notification_text(text: str, context: dict[str, str]) -> str:
    result = text or ""
    for key, val in context.items():
        result = result.replace("{" + key + "}", str(val))
    return result


def _get_popup_queue() -> list[dict]:
    raw = get_app_state("notifications_popup_queue")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_popup_queue(items: list[dict]) -> None:
    set_app_state("notifications_popup_queue", json.dumps(items))


def _enqueue_popup(subject: str, body: str) -> None:
    items = _get_popup_queue()
    items.append(
        {
            "subject": subject,
            "body": body,
            "created_at": _utc_now_iso(),
        }
    )
    _save_popup_queue(items)


def _send_popup_digest(items: list[dict]) -> None:
    if not items:
        return
    recipients = _notification_recipients()
    if not recipients:
        return
    lines = []
    for it in items:
        subj = it.get("subject", "")
        body = it.get("body", "")
        lines.append(f"- {subj}")
        if body:
            lines.append(f"  {body}")
        lines.append("")
    subject = "Notifications summary"
    body = "\n".join(lines).strip()
    _send_notification_email(subject, body, recipients)


def _notification_recipients() -> list[str]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT email FROM notification_emails ORDER BY id ASC")
        rows = cur.fetchall()
    return [r["email"] for r in rows]


def _send_notification_email(subject: str, body: str, recipients: list[str]) -> None:
    host = (get_app_state("smtp_host") or os.environ.get("SMTP_HOST", "")).strip()
    if not host or not recipients:
        return
    port_raw = (get_app_state("smtp_port") or os.environ.get("SMTP_PORT", "587")).strip()
    try:
        port = int(port_raw)
    except ValueError:
        port = 587
    user = (get_app_state("smtp_user") or os.environ.get("SMTP_USER", "")).strip()
    password = (get_app_state("smtp_password") or os.environ.get("SMTP_PASSWORD", "")).strip()
    sender = (get_app_state("smtp_sender") or os.environ.get("SMTP_SENDER", user or "no-reply@airportapp.local")).strip()
    use_tls = (get_app_state("smtp_tls") or "1") != "0"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=10) as smtp:
        if use_tls:
            smtp.starttls()
        if user and password:
            smtp.login(user, password)
        smtp.send_message(msg)


def _log_notification(
    template_id: int | None,
    event_key: str,
    recipients: list[str],
    subject: str,
    body: str,
    success: bool,
    error: str | None = None,
) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO notification_logs (
                template_id, event_key, sent_to, subject, body, success, error, created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                template_id,
                event_key,
                ", ".join(recipients),
                subject,
                body,
                1 if success else 0,
                error,
                _utc_now_iso(),
            ),
        )
        conn.commit()


def send_notification(event_key: str, context: dict[str, str]) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, subject, body, enabled FROM notification_templates WHERE slug = ?",
            (event_key,),
        )
        tpl = cur.fetchone()
    if not tpl or int(tpl["enabled"] or 0) != 1:
        return

    recipients = _notification_recipients()
    if not recipients:
        return

    subject = _render_notification_text(tpl["subject"], context)
    body = _render_notification_text(tpl["body"], context)
    _enqueue_popup(subject, body)

    try:
        _send_notification_email(subject, body, recipients)
        _log_notification(tpl["id"], event_key, recipients, subject, body, True)
    except Exception as exc:
        _log_notification(tpl["id"], event_key, recipients, subject, body, False, str(exc))


def _format_month_label(month_key: str) -> str:
    try:
        dt = datetime.strptime(month_key, "%Y-%m")
        return dt.strftime("%B %Y")
    except Exception:
        return month_key


_ensure_default_notification_templates()


def _log_report_snapshot(report_type: str, date_key: str, user_id: int | None) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO report_snapshots (report_type, date_key, created_by, created_at_utc)
            VALUES (?, ?, ?, ?)
            """,
            (report_type, date_key, user_id, _utc_now_iso()),
        )
        conn.commit()


def _report_snapshot_exists(report_type: str, date_key: str) -> bool:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM report_snapshots WHERE report_type = ? AND date_key = ?",
            (report_type, date_key),
        )
        return cur.fetchone() is not None


def _run_notification_checks() -> None:
    tz = ZoneInfo("Europe/Bratislava")
    now_local = datetime.now(tz)
    last_run_raw = get_app_state("notifications_last_check_ts")
    try:
        last_run = int(last_run_raw) if last_run_raw else 0
    except ValueError:
        last_run = 0
    if int(time.time()) - last_run < 300:
        return
    set_app_state("notifications_last_check_ts", str(int(time.time())))

    if now_local.hour < 8:
        return

    today = now_local.date()

    # Daily report not created check (for yesterday)
    yesterday = today - timedelta(days=1)
    y_key = yesterday.isoformat()
    last_daily_key = get_app_state("notifications_daily_not_created_last")
    if last_daily_key != y_key:
        if not _report_snapshot_exists("daily", y_key):
            send_notification(
                "daily_report_not_created",
                {"ReportDate": y_key},
            )
        set_app_state("notifications_daily_not_created_last", y_key)

    # Monthly report not created check (first day of month for previous month)
    if today.day == 1:
        prev_month = (today.replace(day=1) - timedelta(days=1))
        m_key = prev_month.strftime("%Y-%m")
        last_month_key = get_app_state("notifications_monthly_not_created_last")
        if last_month_key != m_key:
            if not _report_snapshot_exists("monthly", m_key):
                send_notification(
                    "monthly_report_not_created",
                    {"ReportMonth": _format_month_label(m_key)},
                )
            set_app_state("notifications_monthly_not_created_last", m_key)


def _compute_variable_rewards_distribution(year: int, month: int):
    monthly_total = _compute_monthly_airport_total(year, month)
    percent_key = f"variable_rewards_percent_{year}_{month:02d}"
    percent_raw = get_app_state(percent_key) or "100"
    try:
        percent_value = float(percent_raw)
    except ValueError:
        percent_value = 100.0
    percent_value = round(min(100.0, max(0.0, percent_value)))
    reduced_total = monthly_total * (percent_value / 100)

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fullname, nickname, role, active "
            "FROM users ORDER BY fullname COLLATE NOCASE ASC"
        )
        users = cur.fetchall()

    manual_map = {}
    for u in users:
        key = f"variable_rewards_manual_{year}_{month:02d}_{u['id']}"
        raw = get_app_state(key)
        if raw is None:
            continue
        try:
            manual_map[u["id"]] = float(raw)
        except ValueError:
            continue

    active_users = [u for u in users if int(u["active"] or 0) == 1]
    active_manual_sum = sum(
        manual_map.get(u["id"], 0.0) for u in active_users if manual_map.get(u["id"], 0.0) > 0
    )
    active_without_manual = [
        u for u in active_users if manual_map.get(u["id"], 0.0) <= 0
    ]
    remainder = max(0.0, reduced_total - active_manual_sum)
    per_user = remainder / len(active_without_manual) if active_without_manual else 0.0

    computed = []
    for u in users:
        manual_amount = manual_map.get(u["id"], 0.0)
        if int(u["active"] or 0) != 1:
            computed_amount = 0.0
        elif manual_amount > 0:
            computed_amount = manual_amount
        else:
            computed_amount = per_user
        computed.append(
            {
                "id": u["id"],
                "fullname": u["fullname"],
                "nickname": u["nickname"],
                "role": u["role"],
                "active": int(u["active"] or 0),
                "manual_amount": float(manual_amount),
                "computed_amount": float(computed_amount),
            }
        )

    return monthly_total, percent_value, reduced_total, computed


def _save_variable_rewards_snapshot(
    year: int,
    month: int,
    monthly_total: float,
    percent_value: float,
    reduced_total: float,
    computed_users: list[dict],
) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        now = _utc_now_iso()
        for u in computed_users:
            cur.execute(
                """
                INSERT INTO variable_rewards_snapshots (
                    year, month, scope, user_id,
                    total_monthly, percent, reduced_total, manual_amount, computed_amount, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(year, month, scope, user_id) DO UPDATE SET
                    total_monthly = excluded.total_monthly,
                    percent = excluded.percent,
                    reduced_total = excluded.reduced_total,
                    manual_amount = excluded.manual_amount,
                    computed_amount = excluded.computed_amount,
                    created_at_utc = excluded.created_at_utc
                """,
                (
                    year,
                    month,
                    "monthly",
                    u["id"],
                    float(monthly_total),
                    float(percent_value),
                    float(reduced_total),
                    float(u["manual_amount"]),
                    float(u["computed_amount"]),
                    now,
                ),
            )

        cur.execute(
            """
            SELECT user_id, SUM(computed_amount) AS total
            FROM variable_rewards_snapshots
            WHERE year = ? AND scope = 'monthly' AND month BETWEEN 1 AND ?
            GROUP BY user_id
            """,
            (year, month),
        )
        ytd_map = {r["user_id"]: float(r["total"] or 0) for r in cur.fetchall()}
        for u in computed_users:
            ytd_total = ytd_map.get(u["id"], 0.0)
            cur.execute(
                """
                INSERT INTO variable_rewards_snapshots (
                    year, month, scope, user_id,
                    total_monthly, percent, reduced_total, manual_amount, computed_amount, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(year, month, scope, user_id) DO UPDATE SET
                    computed_amount = excluded.computed_amount,
                    created_at_utc = excluded.created_at_utc
                """,
                (
                    year,
                    month,
                    "yearly",
                    u["id"],
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    float(ytd_total),
                    now,
                ),
            )

        conn.commit()


def _send_admin_email_new_user(fullname: str, nickname: str) -> None:
    """
    Sends email notification to admins if SMTP is configured.
    If not configured, it silently skips (app still works).
    """
    host = os.environ.get("SMTP_HOST", "").strip()
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "").strip()
    sender = os.environ.get("SMTP_FROM", user).strip()
    recipients_raw = os.environ.get("ADMIN_NOTIFY_EMAILS", "").strip()

    if not host or not sender or not recipients_raw:
        return

    recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
    if not recipients:
        return

    msg = EmailMessage()
    msg["Subject"] = "AirportApp: New account pending approval"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(
        f"A new account was created and is pending approval:\n\n"
        f"Full name: {fullname}\n"
        f"Nickname: {nickname}\n\n"
        f"Please log in to AirportApp and approve the user in Manage users."
    )

    with smtplib.SMTP(host, port, timeout=10) as smtp:
        smtp.starttls()
        if user and password:
            smtp.login(user, password)
        smtp.send_message(msg)


def _sale_snapshot(conn, sale_id: int) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            s.id,
            a.name AS airline_name,
            a.code AS airline_code,
            d.dest_name AS destination_name,
            d.dest_code AS destination_code,
            s.sold_at_utc,
            s.grand_total AS total_amount,
            s.cash_amount,
            s.card_amount,
            s.payment_method,
            (
                SELECT COUNT(*) FROM sale_items si WHERE si.sale_id = s.id
            ) AS items_count,
            (
                SELECT GROUP_CONCAT(
                    CASE
                        WHEN si.fee_source = 'airline' THEN
                            CASE
                                WHEN COALESCE(af.fee_key, si.fee_key, '') != ''
                                    THEN COALESCE(af.fee_key, si.fee_key) || ' - ' || COALESCE(af.fee_name, si.fee_name, si.fee_key)
                                ELSE COALESCE(af.fee_name, si.fee_name, si.fee_key)
                            END
                        WHEN si.fee_source = 'airport' THEN
                            CASE
                                WHEN COALESCE(apf.fee_key, si.fee_key, '') != ''
                                    THEN COALESCE(apf.fee_key, si.fee_key) || ' - ' || COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                                ELSE COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                            END
                        ELSE
                            CASE
                                WHEN COALESCE(si.fee_key, '') != ''
                                    THEN COALESCE(si.fee_key, '') || ' - ' || COALESCE(si.fee_name, si.fee_key)
                                ELSE COALESCE(si.fee_name, '')
                            END
                    END,
                    char(10)
                )
                FROM sale_items si
                LEFT JOIN airline_fees af ON af.id = si.fee_id AND si.fee_source = 'airline'
                LEFT JOIN airport_service_fees apf ON apf.id = si.fee_id AND si.fee_source = 'airport'
                WHERE si.sale_id = s.id
            ) AS items_label
        FROM sales s
        JOIN airlines a ON a.id = s.airline_id
        LEFT JOIN airline_destinations d ON d.id = s.destination_id
        WHERE s.id = ?
        """,
        (sale_id,),
    )
    row = cur.fetchone()
    if not row:
        return {}
    return dict(row)


def _format_sale_changes(before: dict, after: dict) -> str:
    if not before or not after:
        return "Sale updated."

    def _airline_label(row: dict) -> str:
        if not row.get("airline_name"):
            return "-"
        if row.get("airline_code"):
            return f"{row['airline_name']} ({row['airline_code']})"
        return row["airline_name"]

    def _destination_label(row: dict) -> str:
        name = (row.get("destination_name") or "").strip()
        code = (row.get("destination_code") or "").strip()
        if name and code:
            return f"{name} ({code})"
        if name:
            return name
        if code:
            return code
        return "-"

    changes = []

    if _airline_label(before) != _airline_label(after):
        changes.append(f"Airline: {_airline_label(before)} -> {_airline_label(after)}")
    if _destination_label(before) != _destination_label(after):
        changes.append(
            f"Destination: {_destination_label(before)} -> {_destination_label(after)}"
        )

    for key, label in [
        ("payment_method", "Payment"),
        ("total_amount", "Total"),
        ("cash_amount", "Cash"),
        ("card_amount", "Card"),
    ]:
        if before.get(key) != after.get(key):
            changes.append(f"{label}: {before.get(key)} -> {after.get(key)}")

    if before.get("items_count") != after.get("items_count"):
        changes.append(f"Items Count: {before.get('items_count')} -> {after.get('items_count')}")

    if (before.get("items_label") or "") != (after.get("items_label") or ""):
        changes.append(
            "Items:\n"
            f"FROM:\n{before.get('items_label') or '-'}\n"
            f"TO:\n{after.get('items_label') or '-'}"
        )

    return "\n".join(changes) if changes else "No visible changes."


@app.before_request
def enforce_session_timeout_and_single_user():
    session.setdefault("csrf_token", token_urlsafe(32))

    try:
        _run_notification_checks()
    except Exception:
        pass

    if session.get("logged_in") and session.get("role") == "Admin":
        if not session.get("popup_notifications"):
            queue = _get_popup_queue()
            if queue:
                try:
                    _send_popup_digest(queue)
                except Exception:
                    pass
                session["popup_notifications"] = queue
                set_app_state("notifications_popup_queue", "")

    if not session.get("logged_in"):
        return

    active_sid = get_app_state("active_session_id")
    current_sid = session.get("sid")

    if active_sid and current_sid and active_sid != current_sid:
        log_auth_event(
            user_id=session.get("user_id"),
            nickname=session.get("nickname"),
            fullname=session.get("fullname"),
            role=session.get("role"),
            action="SESSION_REVOKED",
            success=True,
            ip=_client_ip(),
            user_agent=_user_agent(),
            details="Another user signed in. You were logged out automatically.",
        )
        session.clear()
        flash("You were logged out because another user signed in.")
        if request.endpoint not in {"index", "login"}:
            return redirect(url_for("index"))
        return

    now_ts = int(time.time())
    last_ts = session.get("last_activity_ts")

    if last_ts is not None and now_ts - int(last_ts) > 30 * 60:
        log_auth_event(
            user_id=session.get("user_id"),
            nickname=session.get("nickname"),
            fullname=session.get("fullname"),
            role=session.get("role"),
            action="SESSION_EXPIRED",
            success=True,
            ip=_client_ip(),
            user_agent=_user_agent(),
            details="Auto logout after 30 minutes of inactivity.",
        )

        if session.get("sid") and session.get("sid") == get_app_state("active_session_id"):
            delete_app_state("active_session_id")

        session.clear()
        flash("Session expired. Please login again.")
        if request.endpoint not in {"index", "login"}:
            return redirect(url_for("index"))
        return

    session["last_activity_ts"] = now_ts


# -----------------------------------------------------------------------------
# Auth
# -----------------------------------------------------------------------------
@app.get("/", endpoint="index")
def index():
    if session.get("logged_in"):
        return _redirect_after_login(session.get("role"))
    return render_template("index.html")


@app.post("/login", endpoint="login")
def login():
    require_csrf()

    if session.get("logged_in"):
        log_auth_event(
            user_id=session.get("user_id"),
            nickname=session.get("nickname"),
            fullname=session.get("fullname"),
            role=session.get("role"),
            action="LOGOUT",
            success=True,
            ip=_client_ip(),
            user_agent=_user_agent(),
            details="Replaced by another login in the same browser.",
        )
        if session.get("sid") and session.get("sid") == get_app_state("active_session_id"):
            delete_app_state("active_session_id")
        session.clear()

    identifier = _sanitize(request.form.get("nickname"))
    password = request.form.get("password") or ""

    if not identifier or not password:
        flash("❌ Please enter your name/nickname and password.")
        return redirect(url_for("index"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fullname, nickname, password, role, must_change_password, approved "
            "FROM users WHERE nickname = ? OR fullname = ?",
            (identifier, identifier),
        )
        row = cur.fetchone()

    if not row:
        flash("❌ Invalid credentials")
        return redirect(url_for("index"))

    if int(row["approved"]) == 0:
        flash("⏳ Your account is pending approval. Please contact Admin.")
        log_auth_event(
            user_id=row["id"],
            nickname=row["nickname"],
            fullname=row["fullname"],
            role=row["role"],
            action="LOGIN_BLOCKED_NOT_APPROVED",
            success=False,
            ip=_client_ip(),
            user_agent=_user_agent(),
            details="User attempted login but account is not approved.",
        )
        return redirect(url_for("index"))

    ok, upgraded_hash = verify_password_and_upgrade(password, row["password"])
    if not ok:
        flash("❌ Invalid credentials")
        return redirect(url_for("index"))

    if upgraded_hash:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE users SET password = ? WHERE id = ?", (upgraded_hash, row["id"]))
            conn.commit()

    sid = token_urlsafe(16)
    session["logged_in"] = True
    session["user_id"] = row["id"]
    session["nickname"] = row["nickname"]
    session["fullname"] = row["fullname"]
    session["role"] = row["role"]
    session["last_activity_ts"] = int(time.time())
    session["sid"] = sid

    set_app_state("active_session_id", sid)

    log_auth_event(
        user_id=row["id"],
        nickname=row["nickname"],
        fullname=row["fullname"],
        role=row["role"],
        action="LOGIN_SUCCESS",
        success=True,
        ip=_client_ip(),
        user_agent=_user_agent(),
        details="User signed in.",
    )

    if row["must_change_password"]:
        return redirect(url_for("change_password"))

    return _redirect_after_login(row["role"])


@app.get("/logout", endpoint="logout")
def logout():
    if session.get("logged_in"):
        log_auth_event(
            user_id=session.get("user_id"),
            nickname=session.get("nickname"),
            fullname=session.get("fullname"),
            role=session.get("role"),
            action="LOGOUT",
            success=True,
            ip=_client_ip(),
            user_agent=_user_agent(),
            details="User clicked logout.",
        )
        if session.get("sid") and session.get("sid") == get_app_state("active_session_id"):
            delete_app_state("active_session_id")

    session.clear()
    flash("You have been logged out.")
    return redirect(url_for("index"))


@app.route("/register", methods=["GET", "POST"], endpoint="register")
def register():
    if request.method == "GET":
        return render_template("register.html")

    require_csrf()
    fullname = _sanitize(request.form.get("fullname"))
    nickname = _sanitize(request.form.get("nickname"))
    password = request.form.get("password") or ""

    q1 = _sanitize(request.form.get("q1"))
    a1 = _sanitize(request.form.get("a1"))
    q2 = _sanitize(request.form.get("q2"))
    a2 = _sanitize(request.form.get("a2"))
    q3 = _sanitize(request.form.get("q3"))
    a3 = _sanitize(request.form.get("a3"))

    if not PASSWORD_RE.match(password):
        flash("❌ Password must have min 8 chars and include letters, numbers and symbols.")
        return redirect(url_for("register"))

    if not (fullname and nickname and q1 and a1 and q2 and a2 and q3 and a3):
        flash("❌ Please fill all fields including 3 security questions.")
        return redirect(url_for("register"))

    now = _utc_now_iso()

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO users (
                    fullname, nickname, password, role,
                    must_change_password, approved, created_at_utc,
                    q1, a1, q2, a2, q3, a3
                )
                VALUES (?, ?, ?, 'User', 0, 0, ?, ?, ?, ?, ?, ?, ?)
                """,
                (fullname, nickname, hash_password(password), now, q1, a1, q2, a2, q3, a3),
            )
            conn.commit()
    except Exception:
        flash("❌ Nickname already exists.")
        return redirect(url_for("register"))

    try:
        _send_admin_email_new_user(fullname=fullname, nickname=nickname)
    except Exception:
        pass
    try:
        send_notification("new_user_created", {"UserName": fullname})
    except Exception:
        pass

    flash("✅ Account created. Waiting for Admin approval. You can login after approval.")
    return redirect(url_for("index"))


@app.route("/forgot", methods=["GET", "POST"], endpoint="forgot")
def forgot():
    if request.method == "GET":
        return render_template("forgot.html")

    require_csrf()
    nickname = _sanitize(request.form.get("nickname"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE nickname = ?", (nickname,))
        user = cur.fetchone()

    if not user:
        flash("❌ User not found.")
        return redirect(url_for("forgot"))

    a1 = request.form.get("a1") or ""
    a2 = request.form.get("a2") or ""
    a3 = request.form.get("a3") or ""
    new_password = request.form.get("new_password") or ""

    if not (a1 and a2 and a3 and new_password):
        return render_template(
            "forgot.html",
            nickname=nickname,
            q1=user["q1"],
            q2=user["q2"],
            q3=user["q3"],
        )

    if a1 != (user["a1"] or "") or a2 != (user["a2"] or "") or a3 != (user["a3"] or ""):
        flash("❌ Answers do not match.")
        return redirect(url_for("forgot"))

    if not PASSWORD_RE.match(new_password):
        flash("❌ Password must have min 8 chars and include letters, numbers and symbols.")
        return redirect(url_for("forgot"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET password = ?, must_change_password = 0 WHERE id = ?",
            (hash_password(new_password), user["id"]),
        )
        conn.commit()

    flash("✅ Password reset. You can login.")
    return redirect(url_for("index"))


@app.route("/change_password", methods=["GET", "POST"], endpoint="change_password")
@login_required
def change_password():
    if request.method == "GET":
        return render_template("change_password.html")

    require_csrf()
    new_password = request.form.get("new_password") or ""
    if not PASSWORD_RE.match(new_password):
        flash("❌ Password must have min 8 chars and include letters, numbers and symbols.")
        return redirect(url_for("change_password"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET password = ?, must_change_password = 0 WHERE id = ?",
            (hash_password(new_password), session["user_id"]),
        )
        conn.commit()

    flash("✅ Password changed.")
    return _redirect_after_login(session.get("role"))


@app.get("/profile", endpoint="profile")
@login_required
def profile():
    user_id = session.get("user_id")
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, fullname, nickname, role FROM users WHERE id = ?", (user_id,))
        user = cur.fetchone()

        cur.execute(
            """
            SELECT action, ip, user_agent, created_at_utc
            FROM auth_logs
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 10
            """,
            (user_id,),
        )
        logs = cur.fetchall()

    if not user:
        flash("User not found.")
        return redirect(url_for("index"))

    return render_template("profile.html", user=user, logs=logs)


# -----------------------------------------------------------------------------
# Admin hub & pages
# -----------------------------------------------------------------------------
@app.get("/admin_hub", endpoint="admin_hub")
@admin_required
def admin_hub():
    return render_template("admin_hub.html")


@app.get("/user_hub", endpoint="user_hub")
@login_required
def user_hub():
    if session.get("role") in {"Admin", "Deputy"}:
        return redirect(url_for("admin_hub"))
    return render_template("user_hub.html")


@app.get("/admin_page", endpoint="admin_page")
@admin_required
def admin_page():
    return render_template("admin_page.html")


@app.get("/sales", endpoint="sales")
@login_required
def sales():
    return render_template("sales.html")


def _load_sale_fee_data():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, code FROM airlines WHERE active = 1 ORDER BY name COLLATE NOCASE ASC")
        airlines = cur.fetchall()

        cur.execute(
            """
            SELECT id, airline_id, fee_key, fee_name, amount, currency, unit
            FROM airline_fees
            ORDER BY fee_name COLLATE NOCASE ASC
            """
        )
        airline_fees = cur.fetchall()

        cur.execute(
            """
            SELECT id, fee_key, fee_name, amount, currency, unit
            FROM airport_service_fees
            ORDER BY fee_name COLLATE NOCASE ASC
            """
        )
        airport_fees = cur.fetchall()

        cur.execute(
            """
            SELECT id, airline_id, dest_code, dest_name, active
            FROM airline_destinations
            ORDER BY dest_name COLLATE NOCASE ASC
            """
        )
        destinations = cur.fetchall()

    airline_fees_map = {}
    for f in airline_fees:
        airline_fees_map.setdefault(f["airline_id"], []).append(
            {
                "id": f["id"],
                "fee_key": f["fee_key"],
                "fee_name": f["fee_name"],
                "amount": f["amount"],
                "currency": f["currency"],
                "unit": f["unit"],
            }
        )

    airport_fees_list = [
        {
            "id": f["id"],
            "fee_key": f["fee_key"],
            "fee_name": f["fee_name"],
            "amount": f["amount"],
            "currency": f["currency"],
            "unit": f["unit"],
        }
        for f in airport_fees
    ]

    destinations_map = {}
    for d in destinations:
        destinations_map.setdefault(d["airline_id"], []).append(
            {
                "id": d["id"],
                "dest_code": d["dest_code"],
                "dest_name": d["dest_name"],
                "active": d["active"],
            }
        )

    return airlines, airline_fees_map, airport_fees_list, destinations_map


@app.route("/sale/new", methods=["GET", "POST"], endpoint="sale_new")
@login_required
def sale_new():
    if request.method == "GET":
        airlines, airline_fees_map, airport_fees_list, destinations_map = _load_sale_fee_data()
        return render_template(
            "sale_new.html",
            airlines=airlines,
            airline_fees_map=airline_fees_map,
            airport_fees=airport_fees_list,
            destinations_map=destinations_map,
        )

    require_csrf()
    airline_id_raw = request.form.get("airline_id") or ""
    destination_id_raw = request.form.get("destination_id") or ""
    pnr = _sanitize(request.form.get("pnr"))
    passenger_name = _sanitize(request.form.get("passenger_name"))
    ticket_qty_raw = request.form.get("ticket_qty") or "0"
    ticket_amount = _parse_amount(request.form.get("ticket_amount"))
    payment_method = _sanitize(request.form.get("payment_method")).upper() or "CASH"
    sale_group_id = _sanitize(request.form.get("sale_group_id")) or None

    try:
        airline_id = int(airline_id_raw)
        destination_id = int(destination_id_raw)
        ticket_qty = max(0, int(ticket_qty_raw))
    except ValueError:
        flash("Invalid input.")
        return redirect(url_for("sale_new"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, code FROM airlines WHERE id = ?", (airline_id,))
        airline_row = cur.fetchone()
        if not airline_row:
            flash("Airline not found.")
            return redirect(url_for("sale_new"))

        cur.execute(
            """
            SELECT id, dest_name, dest_code, active
            FROM airline_destinations
            WHERE id = ? AND airline_id = ?
            """,
            (destination_id, airline_id),
        )
        destination_row = cur.fetchone()
        if not destination_row:
            flash("Destination not found for selected airline.")
            return redirect(url_for("sale_new"))

        if payment_method not in {"CASH", "CARD"}:
            flash("Invalid payment method.")
            return redirect(url_for("sale_new"))

        if not sale_group_id:
            sale_group_id = token_urlsafe(8)

        now = _utc_now_iso()
        items = []

        airline_fee_ids = request.form.getlist("airline_fee_id")
        for fid_raw in airline_fee_ids:
            try:
                fid = int(fid_raw)
            except ValueError:
                continue
            qty = max(1, int(request.form.get(f"airline_qty_{fid}") or "1"))
            cur.execute(
                """
                SELECT id, fee_key, fee_name, amount, currency
                FROM airline_fees
                WHERE id = ? AND airline_id = ?
                """,
                (fid, airline_id),
            )
            fee = cur.fetchone()
            if not fee:
                continue
            amount = float(fee["amount"] or 0)
            total = round(amount * qty, 4)
            items.append(
                {
                    "fee_source": "airline",
                    "fee_id": fee["id"],
                    "fee_key": fee["fee_key"],
                    "fee_name": fee["fee_name"],
                    "amount": amount,
                    "currency": fee["currency"] or "EUR",
                    "quantity": qty,
                    "total_amount": total,
                }
            )

        airport_fee_ids = request.form.getlist("airport_fee_id")
        for fid_raw in airport_fee_ids:
            try:
                fid = int(fid_raw)
            except ValueError:
                continue
            qty = max(1, int(request.form.get(f"airport_qty_{fid}") or "1"))
            cur.execute(
                """
                SELECT id, fee_key, fee_name, amount, currency
                FROM airport_service_fees
                WHERE id = ?
                """,
                (fid,),
            )
            fee = cur.fetchone()
            if not fee:
                continue
            amount = float(fee["amount"] or 0)
            total = round(amount * qty, 4)
            items.append(
                {
                    "fee_source": "airport",
                    "fee_id": fee["id"],
                    "fee_key": fee["fee_key"],
                    "fee_name": fee["fee_name"],
                    "amount": amount,
                    "currency": fee["currency"] or "EUR",
                    "quantity": qty,
                    "total_amount": total,
                }
            )

        airline_label = (
            f"{airline_row['name']} ({airline_row['code']})"
            if airline_row["code"]
            else airline_row["name"]
        )

        if ticket_qty > 0 and ticket_amount > 0:
            ticket_total = round(ticket_amount * ticket_qty, 4)
            items.append(
                {
                    "fee_source": "ticket",
                    "fee_id": 0,
                    "fee_key": "TICKET",
                    "fee_name": f"{airline_label} Plane Ticket",
                    "amount": ticket_amount,
                    "currency": "EUR",
                    "quantity": ticket_qty,
                    "total_amount": ticket_total,
                }
            )

        if not items:
            flash("Select at least one fee.")
            return redirect(url_for("sale_new"))

        grand_total = round(sum(i["total_amount"] for i in items), 4)

        def _split_payment(total: float) -> tuple[float, float]:
            if payment_method == "CARD":
                return 0.0, total
            return total, 0.0

        cash_amount, card_amount = _split_payment(grand_total)

        cur.execute(
            """
            INSERT INTO sales (
                sale_group_id, airline_id, destination_id, pnr, passenger_name, sold_at_utc, created_by,
                payment_method, cash_amount, card_amount, grand_total,
                fee_source, fee_id, fee_name, amount, currency, quantity, total_amount
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sale_group_id,
                airline_id,
                destination_id,
                pnr or None,
                passenger_name or None,
                now,
                session.get("user_id"),
                payment_method,
                cash_amount,
                card_amount,
                grand_total,
                "multi",
                0,
                "MULTI",
                grand_total,
                "EUR",
                1,
                grand_total,
            ),
        )
        sale_id = cur.lastrowid
        for item in items:
            cur.execute(
                """
                INSERT INTO sale_items (
                    sale_id, fee_source, fee_id, fee_key, fee_name,
                    amount, currency, quantity, total_amount, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sale_id,
                    item["fee_source"],
                    item["fee_id"],
                    item["fee_key"],
                    item["fee_name"],
                    item["amount"],
                    item["currency"],
                    item["quantity"],
                    item["total_amount"],
                    now,
                ),
            )
        conn.commit()

    flash("Sale saved.")
    return redirect(url_for("sale_new"))


@app.get("/sales_list", endpoint="sales_list")
@login_required
def sales_list():
    q_raw = _sanitize(request.args.get("q"))
    q = f"%{q_raw}%" if q_raw else ""
    with get_connection() as conn:
        cur = conn.cursor()
        sql = """
            SELECT
                s.id,
                s.sale_group_id,
                s.pnr,
                s.passenger_name,
                a.name AS airline_name,
                a.code AS airline_code,
                d.dest_name AS destination_name,
                d.dest_code AS destination_code,
                s.sold_at_utc,
                s.grand_total AS total_amount,
                s.cash_amount,
                s.card_amount,
                s.payment_method,
                u.fullname AS sold_by_name,
                u.nickname AS sold_by_nick,
                (
                    SELECT COUNT(*) FROM sale_items si WHERE si.sale_id = s.id
                ) AS items_count,
                (
                    SELECT GROUP_CONCAT(
                        CASE
                            WHEN si.fee_source = 'airline' THEN
                                CASE
                                    WHEN COALESCE(af.fee_key, si.fee_key, '') != ''
                                        THEN COALESCE(af.fee_key, si.fee_key) || ' - ' || COALESCE(af.fee_name, si.fee_name, si.fee_key)
                                    ELSE COALESCE(af.fee_name, si.fee_name, si.fee_key)
                                END
                            WHEN si.fee_source = 'airport' THEN
                                CASE
                                    WHEN COALESCE(apf.fee_key, si.fee_key, '') != ''
                                        THEN COALESCE(apf.fee_key, si.fee_key) || ' - ' || COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                                    ELSE COALESCE(apf.fee_name, si.fee_name, si.fee_key)
                                END
                            ELSE
                                CASE
                                    WHEN COALESCE(si.fee_key, '') != ''
                                        THEN COALESCE(si.fee_key, '') || ' - ' || COALESCE(si.fee_name, si.fee_key)
                                    ELSE COALESCE(si.fee_name, '')
                                END
                        END,
                        char(10)
                    )
                    FROM sale_items si
                    LEFT JOIN airline_fees af ON af.id = si.fee_id AND si.fee_source = 'airline'
                    LEFT JOIN airport_service_fees apf ON apf.id = si.fee_id AND si.fee_source = 'airport'
                    WHERE si.sale_id = s.id
                ) AS items_label
            FROM sales s
            JOIN airlines a ON a.id = s.airline_id
            LEFT JOIN airline_destinations d ON d.id = s.destination_id
            LEFT JOIN users u ON u.id = s.created_by
        """
        params = []
        if q:
            sql += " WHERE (s.pnr LIKE ? OR s.passenger_name LIKE ?)"
            params.extend([q, q])
        sql += " ORDER BY s.id DESC"
        cur.execute(sql, params)
        rows = cur.fetchall()
    return render_template("sales_list.html", sales=rows, q=q_raw)


@app.route("/sales/<int:sale_id>/edit", methods=["GET", "POST"], endpoint="sale_edit")
@login_required
def sale_edit(sale_id: int):
    if request.method == "GET":
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM sales WHERE id = ?", (sale_id,))
            sale = cur.fetchone()
            cur.execute(
                """
                SELECT fee_source, fee_id, quantity, amount
                FROM sale_items
                WHERE sale_id = ?
                """,
                (sale_id,),
            )
            items = [dict(r) for r in cur.fetchall()]
        if not sale:
            flash("Sale not found.")
            return redirect(url_for("sales_list"))

        airlines, airline_fees_map, airport_fees_list, destinations_map = _load_sale_fee_data()
        return render_template(
            "sale_edit.html",
            sale=sale,
            items=items,
            airlines=airlines,
            airline_fees_map=airline_fees_map,
            airport_fees=airport_fees_list,
            destinations_map=destinations_map,
        )

    require_csrf()
    airline_id_raw = request.form.get("airline_id") or ""
    destination_id_raw = request.form.get("destination_id") or ""
    pnr = _sanitize(request.form.get("pnr"))
    passenger_name = _sanitize(request.form.get("passenger_name"))
    ticket_qty_raw = request.form.get("ticket_qty") or "0"
    ticket_amount = _parse_amount(request.form.get("ticket_amount"))
    sale_group_id = _sanitize(request.form.get("sale_group_id")) or None
    payment_method = _sanitize(request.form.get("payment_method")).upper() or "CASH"

    try:
        airline_id = int(airline_id_raw)
        destination_id = int(destination_id_raw)
        ticket_qty = max(0, int(ticket_qty_raw))
    except ValueError:
        flash("Invalid input.")
        return redirect(url_for("sale_edit", sale_id=sale_id))

    with get_connection() as conn:
        cur = conn.cursor()
        before_snapshot = _sale_snapshot(conn, sale_id)
        cur.execute("SELECT id, name, code FROM airlines WHERE id = ?", (airline_id,))
        airline_row = cur.fetchone()
        if not airline_row:
            flash("Airline not found.")
            return redirect(url_for("sale_edit", sale_id=sale_id))

        cur.execute(
            """
            SELECT id, dest_name, dest_code, active
            FROM airline_destinations
            WHERE id = ? AND airline_id = ?
            """,
            (destination_id, airline_id),
        )
        destination_row = cur.fetchone()
        if not destination_row:
            flash("Destination not found for selected airline.")
            return redirect(url_for("sale_edit", sale_id=sale_id))

        if payment_method not in {"CASH", "CARD"}:
            flash("Invalid payment method.")
            return redirect(url_for("sale_edit", sale_id=sale_id))

        items = []
        airline_fee_ids = request.form.getlist("airline_fee_id")
        for fid_raw in airline_fee_ids:
            try:
                fid = int(fid_raw)
            except ValueError:
                continue
            qty = max(1, int(request.form.get(f"airline_qty_{fid}") or "1"))
            cur.execute(
                """
                SELECT id, fee_key, fee_name, amount, currency
                FROM airline_fees
                WHERE id = ? AND airline_id = ?
                """,
                (fid, airline_id),
            )
            fee = cur.fetchone()
            if not fee:
                continue
            amount = float(fee["amount"] or 0)
            total = round(amount * qty, 4)
            items.append(
                {
                    "fee_source": "airline",
                    "fee_id": fee["id"],
                    "fee_key": fee["fee_key"],
                    "fee_name": fee["fee_name"],
                    "amount": amount,
                    "currency": fee["currency"] or "EUR",
                    "quantity": qty,
                    "total_amount": total,
                }
            )

        airport_fee_ids = request.form.getlist("airport_fee_id")
        for fid_raw in airport_fee_ids:
            try:
                fid = int(fid_raw)
            except ValueError:
                continue
            qty = max(1, int(request.form.get(f"airport_qty_{fid}") or "1"))
            cur.execute(
                """
                SELECT id, fee_key, fee_name, amount, currency
                FROM airport_service_fees
                WHERE id = ?
                """,
                (fid,),
            )
            fee = cur.fetchone()
            if not fee:
                continue
            amount = float(fee["amount"] or 0)
            total = round(amount * qty, 4)
            items.append(
                {
                    "fee_source": "airport",
                    "fee_id": fee["id"],
                    "fee_key": fee["fee_key"],
                    "fee_name": fee["fee_name"],
                    "amount": amount,
                    "currency": fee["currency"] or "EUR",
                    "quantity": qty,
                    "total_amount": total,
                }
            )

        airline_label = (
            f"{airline_row['name']} ({airline_row['code']})"
            if airline_row["code"]
            else airline_row["name"]
        )

        if ticket_qty > 0 and ticket_amount > 0:
            ticket_total = round(ticket_amount * ticket_qty, 4)
            items.append(
                {
                    "fee_source": "ticket",
                    "fee_id": 0,
                    "fee_key": "TICKET",
                    "fee_name": f"{airline_label} Plane Ticket",
                    "amount": ticket_amount,
                    "currency": "EUR",
                    "quantity": ticket_qty,
                    "total_amount": ticket_total,
                }
            )

        if not items:
            flash("Select at least one fee.")
            return redirect(url_for("sale_edit", sale_id=sale_id))

        grand_total = round(sum(i["total_amount"] for i in items), 4)
        now = _utc_now_iso()
        if payment_method == "CARD":
            cash_amount, card_amount = 0.0, grand_total
        else:
            cash_amount, card_amount = grand_total, 0.0

        cur.execute(
            """
            UPDATE sales
            SET sale_group_id = ?, airline_id = ?, destination_id = ?, pnr = ?, passenger_name = ?,
                sold_at_utc = ?, payment_method = ?,
                cash_amount = ?, card_amount = ?, grand_total = ?,
                fee_source = ?, fee_id = ?, fee_name = ?, amount = ?, currency = ?, quantity = ?, total_amount = ?
            WHERE id = ?
            """,
            (
                sale_group_id,
                airline_id,
                destination_id,
                pnr or None,
                passenger_name or None,
                now,
                payment_method,
                cash_amount,
                card_amount,
                grand_total,
                "multi",
                0,
                "MULTI",
                grand_total,
                "EUR",
                1,
                grand_total,
                sale_id,
            ),
        )
        cur.execute("DELETE FROM sale_items WHERE sale_id = ?", (sale_id,))
        for item in items:
            cur.execute(
                """
                INSERT INTO sale_items (
                    sale_id, fee_source, fee_id, fee_key, fee_name,
                    amount, currency, quantity, total_amount, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sale_id,
                    item["fee_source"],
                    item["fee_id"],
                    item["fee_key"],
                    item["fee_name"],
                    item["amount"],
                    item["currency"],
                    item["quantity"],
                    item["total_amount"],
                    now,
                ),
            )
        conn.commit()

        after_snapshot = _sale_snapshot(conn, sale_id)
        details = _format_sale_changes(before_snapshot, after_snapshot)

        log_sales_event(
            user_id=session.get("user_id"),
            sale_id=sale_id,
            action="SALE_EDIT",
            details=details,
            ip=_client_ip(),
            user_agent=_user_agent(),
        )

    flash("Sale updated.")
    return redirect(url_for("sales_list"))


@app.post("/sales/<int:sale_id>/delete", endpoint="sales_delete")
@admin_required
def sales_delete(sale_id: int):
    require_csrf()
    with get_connection() as conn:
        cur = conn.cursor()
        before_snapshot = _sale_snapshot(conn, sale_id)
        cur.execute("DELETE FROM sales WHERE id = ?", (sale_id,))
        conn.commit()
    if before_snapshot:
        dest_name = (before_snapshot.get("destination_name") or "").strip()
        dest_code = (before_snapshot.get("destination_code") or "").strip()
        if dest_name and dest_code:
            dest_label = f"{dest_name} ({dest_code})"
        elif dest_name:
            dest_label = dest_name
        elif dest_code:
            dest_label = dest_code
        else:
            dest_label = "-"
        details = (
            f"Deleted sale: airline={before_snapshot.get('airline_name')} "
            f"{'(' + before_snapshot.get('airline_code') + ')' if before_snapshot.get('airline_code') else ''}; "
            f"destination={dest_label}; "
            f"items_count={before_snapshot.get('items_count')}; "
            f"total={before_snapshot.get('total_amount')}; "
            f"cash={before_snapshot.get('cash_amount')}; "
            f"card={before_snapshot.get('card_amount')}; "
            f"payment={before_snapshot.get('payment_method')}\n"
            f"Items:\n{before_snapshot.get('items_label') or '-'}"
        )
    else:
        details = "Deleted sale."
    log_sales_event(
        user_id=session.get("user_id"),
        sale_id=sale_id,
        action="SALE_DELETE",
        details=details,
        ip=_client_ip(),
        user_agent=_user_agent(),
    )
    flash("Sale deleted.")
    return redirect(url_for("sales_list"))



@app.get("/reports", endpoint="reports")
@login_required
def reports():
    return render_template("reports.html")


@app.get("/reports/daily", endpoint="reports_daily")
@login_required
def reports_daily():
    date_str = _sanitize(request.args.get("date")) or _today_utc_date()
    data = _build_report_payload(date_str, is_month=False)
    return render_template("report_daily.html", date_str=date_str, **data)


@app.get("/reports/monthly", endpoint="reports_monthly")
@login_required
def reports_monthly():
    month_str = _sanitize(request.args.get("month")) or _month_utc()
    data = _build_report_payload(month_str, is_month=True)
    return render_template("report_monthly.html", month_str=month_str, **data)


@app.get("/reports/custom", endpoint="reports_custom")
@login_required
def reports_custom():
    airlines, airline_items, airport_items, sellers, destinations = _load_custom_report_filters()
    _, airline_fees_map, airport_fees_list, destinations_map = _load_sale_fee_data()
    airlines_json = [dict(a) for a in airlines]
    airport_items_json = [dict(a) for a in airport_items]
    destinations_json = [dict(d) for d in destinations]

    filters, selected = _parse_custom_report_filters(request.args)
    rows, chart_data = _build_custom_report(filters)
    palette = [
        "#0ea5e9", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
        "#14b8a6", "#f97316", "#22c55e", "#eab308", "#06b6d4",
    ]
    for idx, s in enumerate(chart_data.get("series", [])):
        s["color"] = palette[idx % len(palette)]

    airline_items_summary = (
        _custom_report_items_by_source(filters, "airline") if filters["include_airline"] else []
    )
    airport_items_summary = (
        _custom_report_items_by_source(filters, "airport") if filters["include_airport"] else []
    )
    airline_totals = (
        _custom_report_totals_by_airline(filters, "airline") if filters["include_airline"] else []
    )
    airport_totals = (
        _custom_report_totals_by_airline(filters, "airport") if filters["include_airport"] else []
    )
    airline_all = (
        _custom_report_total_all(filters, "airline")
        if filters["include_airline"]
        else {"total": 0.0, "cash_total": 0.0, "card_total": 0.0}
    )
    airport_all = (
        _custom_report_total_all(filters, "airport")
        if filters["include_airport"]
        else {"total": 0.0, "cash_total": 0.0, "card_total": 0.0}
    )
    combined = {
        "total": airline_all["total"] + airport_all["total"],
        "cash_total": airline_all["cash_total"] + airport_all["cash_total"],
        "card_total": airline_all["card_total"] + airport_all["card_total"],
    }

    airlines_by_id = {str(a["id"]): a for a in airlines}
    destinations_by_id = {str(d["id"]): d for d in destinations}
    sellers_by_id = {str(u["id"]): u for u in sellers}
    airline_fee_label_map = {}
    for airline_id, fees in airline_fees_map.items():
        airline = airlines_by_id.get(str(airline_id))
        airline_label = airline["name"] if airline else f"Airline {airline_id}"
        if airline and airline["code"]:
            airline_label = f"{airline_label} ({airline['code']})"
        for f in fees:
            airline_fee_label_map[str(f["id"])] = f"{airline_label} - {f['fee_key']} - {f['fee_name']}"
    airport_fee_label_map = {
        str(f["id"]): f"Airport - {f['fee_key']} - {f['fee_name']}" for f in airport_fees_list
    }

    selected_airline_labels = []
    for aid in selected["selected_airlines"]:
        if aid == "airport":
            continue
        a = airlines_by_id.get(str(aid))
        if a:
            label = a["name"]
            if a["code"]:
                label = f"{label} ({a['code']})"
            selected_airline_labels.append(label)

    selected_item_labels = []
    for v in selected["selected_items"]:
        if v == "ticket":
            selected_item_labels.append("Plane Ticket")
        elif v.startswith("airline:"):
            fid = v.split(":", 1)[1]
            label = airline_fee_label_map.get(fid)
            if label:
                selected_item_labels.append(label)
        elif v.startswith("ticket:"):
            aid = v.split(":", 1)[1]
            a = airlines_by_id.get(str(aid))
            if a:
                label = a["name"]
                if a["code"]:
                    label = f"{label} ({a['code']})"
                selected_item_labels.append(f"{label} Plane Ticket")
        elif v.startswith("airport:"):
            fid = v.split(":", 1)[1]
            label = airport_fee_label_map.get(fid)
            if label:
                selected_item_labels.append(label)

    selected_seller_labels = []
    for sid in selected["selected_sellers"]:
        u = sellers_by_id.get(str(sid))
        if u:
            selected_seller_labels.append(u["fullname"] or u["nickname"])

    selected_destination_labels = []
    for did in selected["selected_destinations"]:
        d = destinations_by_id.get(str(did))
        if not d:
            continue
        name = d["dest_name"] or ""
        code = d["dest_code"] or ""
        if name and code:
            selected_destination_labels.append(f"{name} ({code})")
        elif name:
            selected_destination_labels.append(name)
        elif code:
            selected_destination_labels.append(code)

    source_labels = []
    if "airline" in selected["selected_sources"]:
        source_labels.append("Airline Fees")
    if "airport" in selected["selected_sources"] or "airport" in selected["selected_airlines"]:
        source_labels.append("Airport Fees")

    chart_title_parts = []
    if selected["selected_airlines"]:
        names = []
        for a in airlines:
            if str(a["id"]) in selected["selected_airlines"]:
                names.append(a["name"])
        if names:
            chart_title_parts.append(" + ".join(names))
    if selected["selected_destinations"]:
        dest_names = []
        for d in destinations:
            if str(d["id"]) in selected["selected_destinations"]:
                label = d["dest_name"] or ""
                if d["dest_code"]:
                    label = f"{label} ({d['dest_code']})" if label else d["dest_code"]
                if label:
                    dest_names.append(label)
        if dest_names:
            chart_title_parts.append(" | ".join(dest_names))
    if "airport" in selected["selected_sources"] or "airport" in selected["selected_airlines"]:
        chart_title_parts.append("Airport Service Fees")
    chart_title = " + ".join(chart_title_parts) if chart_title_parts else "Custom Report Chart"

    return render_template(
        "report_custom.html",
        date_from=selected["date_from"],
        date_to=selected["date_to"],
        airlines=airlines,
        airline_items=airline_items,
        airport_items=airport_items,
        airport_fees=airport_items_json,
        airlines_json=airlines_json,
        destinations=destinations,
        destinations_json=destinations_json,
        destinations_map=destinations_map,
        sellers=sellers,
        airline_fees_map=airline_fees_map,
        airport_fees_list=airport_fees_list,
        selected_sources=selected["selected_sources"],
        selected_airlines=selected["selected_airlines"],
        selected_destinations=selected["selected_destinations"],
        show_destination=bool(selected["selected_destinations"]),
        selected_items=selected["selected_items"],
        selected_payments=selected["selected_payments"],
        selected_sellers=selected["selected_sellers"],
        selected_airline_labels=selected_airline_labels,
        selected_destination_labels=selected_destination_labels,
        selected_item_labels=selected_item_labels,
        selected_seller_labels=selected_seller_labels,
        selected_payment_labels=(
            selected["selected_payments"]
            if selected["selected_payments"]
            else ["TOTAL (CASH + CARD)"]
        ),
        selected_source_labels=source_labels,
        airline_items_summary=airline_items_summary,
        airport_items_summary=airport_items_summary,
        airline_totals=airline_totals,
        airport_totals=airport_totals,
        airline_all=airline_all,
        airport_all=airport_all,
        combined_all=combined,
        chart_data=chart_data,
        chart_title=chart_title,
    )


def _report_to_csv(rows):
    output = StringIO()
    writer = csv.writer(output, delimiter=";")
    for r in rows:
        writer.writerow(r)
    return output.getvalue().encode("utf-8")


def _report_to_pdf(title: str, rows):
    buffer = BytesIO()
    if "Vera" not in pdfmetrics.getRegisteredFontNames():
        font_dir = os.path.join(PROJECT_ROOT, "assets", "fonts")
        pdfmetrics.registerFont(TTFont("Vera", os.path.join(font_dir, "Vera.ttf")))
        pdfmetrics.registerFont(TTFont("Vera-Bold", os.path.join(font_dir, "VeraBd.ttf")))
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36,
    )
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Title"],
        fontName="Vera-Bold",
        fontSize=16,
        leading=20,
        spaceAfter=12,
        textColor=colors.black,
    )
    section_style = ParagraphStyle(
        "SectionTitle",
        parent=styles["Heading2"],
        fontName="Vera-Bold",
        fontSize=12,
        leading=14,
        spaceBefore=6,
        spaceAfter=6,
        textColor=colors.black,
    )
    normal_style = ParagraphStyle(
        "NormalCell",
        parent=styles["BodyText"],
        fontName="Vera",
        fontSize=9,
        leading=11,
        textColor=colors.black,
    )

    def make_section_header(text):
        header = Table([[Paragraph(text, section_style)]], colWidths=[doc.width])
        header.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f1f5f9")),
                    ("BOX", (0, 0), (-1, -1), 1, colors.HexColor("#cbd5e1")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        return header

    def wrap_table_data(data):
        wrapped = []
        for row in data:
            wrapped.append([Paragraph(str(cell), normal_style) for cell in row])
        return wrapped

    def make_table(data, col_widths, header=True, total_row=False):
        t = Table(wrap_table_data(data), colWidths=col_widths)
        style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.white),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
        if header:
            style.add("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0"))
            style.add("TEXTCOLOR", (0, 0), (-1, 0), colors.black)
            style.add("FONTNAME", (0, 0), (-1, 0), "Vera-Bold")
        if total_row:
            style.add("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc"))
            style.add("FONTNAME", (0, 1), (-1, 1), "Vera-Bold")
            style.add("FONTSIZE", (0, 1), (-1, 1), 12)
        t.setStyle(style)
        return t

    elements = [Paragraph(title, title_style)]

    # parse rows to sections + tables
    sections = []
    i = 0
    while i < len(rows):
        row = rows[i]
        if not row:
            i += 1
            continue
        if len(row) == 1 and isinstance(row[0], str):
            heading = row[0]
            table_rows = []
            i += 1
            while i < len(rows):
                r2 = rows[i]
                if not r2:
                    break
                if len(r2) == 1 and isinstance(r2[0], str):
                    break
                table_rows.append(r2)
                i += 1
            sections.append((heading, table_rows))
            continue
        i += 1

    page_width = doc.width
    for heading, table_rows in sections:
        elements.append(make_section_header(heading))
        if not table_rows:
            elements.append(Spacer(1, 6))
            continue

        header = table_rows[0]
        data_rows = table_rows[1:]
        if header == ["Airline", "Destination", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"]:
            col_widths = [
                page_width * 0.16,
                page_width * 0.18,
                page_width * 0.12,
                page_width * 0.22,
                page_width * 0.06,
                page_width * 0.10,
                page_width * 0.08,
                page_width * 0.08,
            ]
            elements.append(make_table([header] + data_rows, col_widths, header=True))
        elif header == ["Airline", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"]:
            col_widths = [
                page_width * 0.18,
                page_width * 0.14,
                page_width * 0.26,
                page_width * 0.08,
                page_width * 0.12,
                page_width * 0.11,
                page_width * 0.11,
            ]
            elements.append(make_table([header] + data_rows, col_widths, header=True))
        elif header == ["Airline", "Total", "Cash", "Card"]:
            col_widths = [
                page_width * 0.46,
                page_width * 0.18,
                page_width * 0.18,
                page_width * 0.18,
            ]
            elements.append(make_table([header] + data_rows, col_widths, header=True))
        elif header == ["Total", "Cash", "Card"] and len(data_rows) == 1:
            totals_table = [header] + data_rows
            col_widths = [page_width * 0.34, page_width * 0.33, page_width * 0.33]
            elements.append(make_table(totals_table, col_widths, header=True, total_row=True))
        else:
            col_count = max(len(r) for r in table_rows)
            col_widths = [page_width / col_count] * col_count
            elements.append(make_table(table_rows, col_widths, header=True))

        elements.append(Spacer(1, 10))

    doc.build(elements)
    return buffer.getvalue()


def _export_report(date_filter: str, is_month: bool, fmt: str):
    data = _build_report_payload(date_filter, is_month)
    rows = []
    label = "Monthly" if is_month else "Daily"
    report_type = "monthly" if is_month else "daily"
    date_key = date_filter
    _log_report_snapshot(report_type, date_key, session.get("user_id"))
    try:
        user_name = session.get("fullname") or session.get("nickname") or "User"
        if is_month:
            send_notification(
                "monthly_report_created",
                {"UserName": user_name, "ReportMonth": _format_month_label(date_key)},
            )
        else:
            send_notification(
                "daily_report_created",
                {"UserName": user_name, "ReportDate": date_key},
            )
    except Exception:
        pass

    def _destination_label(row):
        keys = row.keys() if hasattr(row, "keys") else []
        name = (row["destination_name"] or "").strip() if "destination_name" in keys else ""
        code = (row["destination_code"] or "").strip() if "destination_code" in keys else ""
        if name and code:
            return f"{name} ({code})"
        if name:
            return name
        if code:
            return code
        return "-"

    rows.append([f"{label} Report", date_filter])
    rows.append([])
    rows.append(["Airline Fees"])
    rows.append(["Airline", "Destination", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"])
    for r in data["airline_items"]:
        airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
        destination = _destination_label(r)
        rows.append(
            [
                airline,
                destination,
                r["fee_key"],
                r["fee_name"],
                r["qty"],
                r["total"],
                r["cash_total"],
                r["card_total"],
            ]
        )
    rows.append([])
    rows.append(["Airline Fees Totals by Airline"])
    rows.append(["Airline", "Total", "Cash", "Card"])
    for r in data["airline_totals"]:
        airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
        rows.append([airline, r["total"], r["cash_total"], r["card_total"]])
    rows.append(["Airline Fees Total (All)"])
    rows.append(["Total", "Cash", "Card"])
    rows.append([data["airline_all"]["total"], data["airline_all"]["cash_total"], data["airline_all"]["card_total"]])
    rows.append([])
    rows.append(["Airport Service Fees"])
    rows.append(["Airline", "Destination", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"])
    for r in data["airport_items"]:
        airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
        destination = _destination_label(r)
        rows.append(
            [
                airline,
                destination,
                r["fee_key"],
                r["fee_name"],
                r["qty"],
                r["total"],
                r["cash_total"],
                r["card_total"],
            ]
        )
    rows.append([])
    rows.append(["Airport Fees Totals by Airline"])
    rows.append(["Airline", "Total", "Cash", "Card"])
    for r in data["airport_totals"]:
        airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
        rows.append([airline, r["total"], r["cash_total"], r["card_total"]])
    rows.append(["Airport Fees Total (All)"])
    rows.append(["Total", "Cash", "Card"])
    rows.append([data["airport_all"]["total"], data["airport_all"]["cash_total"], data["airport_all"]["card_total"]])
    rows.append([])
    rows.append(["All Fees Total"])
    rows.append(["Total", "Cash", "Card"])
    rows.append([data["combined_all"]["total"], data["combined_all"]["cash_total"], data["combined_all"]["card_total"]])

    if fmt == "csv":
        content = _report_to_csv(rows)
        resp = make_response(content)
        resp.headers["Content-Type"] = "text/csv"
        resp.headers["Content-Disposition"] = f"attachment; filename={label.lower()}_report_{date_filter}.csv"
        return resp
    if fmt == "pdf":
        content = _report_to_pdf(f"{label} Report {date_filter}", rows)
        resp = make_response(content)
        resp.headers["Content-Type"] = "application/pdf"
        if is_month:
            filename = f"[MONTHLY REPORT] {date_filter}.pdf"
        else:
            filename = f"[DAILY REPORT] {date_filter}.pdf"
        resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return resp
    abort(400)


@app.get("/reports/daily/export", endpoint="reports_daily_export")
@login_required
def reports_daily_export():
    date_str = _sanitize(request.args.get("date")) or _today_utc_date()
    fmt = _sanitize(request.args.get("format")) or "csv"
    return _export_report(date_str, is_month=False, fmt=fmt.lower())


@app.get("/reports/monthly/export", endpoint="reports_monthly_export")
@login_required
def reports_monthly_export():
    month_str = _sanitize(request.args.get("month")) or _month_utc()
    fmt = _sanitize(request.args.get("format")) or "csv"
    return _export_report(month_str, is_month=True, fmt=fmt.lower())


@app.get("/reports/custom/export", endpoint="reports_custom_export")
@login_required
def reports_custom_export():
    filters, selected = _parse_custom_report_filters(request.args)
    fmt = _sanitize(request.args.get("format")) or "csv"

    airline_items_summary = (
        _custom_report_items_by_source(filters, "airline") if filters["include_airline"] else []
    )
    airport_items_summary = (
        _custom_report_items_by_source(filters, "airport") if filters["include_airport"] else []
    )
    airline_totals = (
        _custom_report_totals_by_airline(filters, "airline") if filters["include_airline"] else []
    )
    airport_totals = (
        _custom_report_totals_by_airline(filters, "airport") if filters["include_airport"] else []
    )
    airline_all = (
        _custom_report_total_all(filters, "airline")
        if filters["include_airline"]
        else {"total": 0.0, "cash_total": 0.0, "card_total": 0.0}
    )
    airport_all = (
        _custom_report_total_all(filters, "airport")
        if filters["include_airport"]
        else {"total": 0.0, "cash_total": 0.0, "card_total": 0.0}
    )
    combined = {
        "total": airline_all["total"] + airport_all["total"],
        "cash_total": airline_all["cash_total"] + airport_all["cash_total"],
        "card_total": airline_all["card_total"] + airport_all["card_total"],
    }
    _, chart_data = _build_custom_report(filters)

    def _destination_label(row):
        keys = row.keys() if hasattr(row, "keys") else []
        name = (row["destination_name"] or "").strip() if "destination_name" in keys else ""
        code = (row["destination_code"] or "").strip() if "destination_code" in keys else ""
        if name and code:
            return f"{name} ({code})"
        if name:
            return name
        if code:
            return code
        return "-"

    # Structured rows used for PDF export
    rows = []
    rows.append([f"Custom Report", f"{filters['date_from']} to {filters['date_to']}"])
    rows.append([])
    if filters["include_airline"]:
        rows.append(["Airline Fees"])
        rows.append(["Airline", "Destination", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"])
        for r in airline_items_summary:
            airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
            destination = _destination_label(r)
            rows.append(
                [
                    airline,
                    destination,
                    r["fee_key"],
                    r["fee_name"],
                    r["qty"],
                    r["total"],
                    r["cash_total"],
                    r["card_total"],
                ]
            )
        rows.append([])
        rows.append(["Airline Fees Totals by Airline"])
        rows.append(["Airline", "Total", "Cash", "Card"])
        for r in airline_totals:
            airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
            rows.append([airline, r["total"], r["cash_total"], r["card_total"]])
        rows.append(["Airline Fees Total (All)"])
        rows.append(["Total", "Cash", "Card"])
        rows.append([airline_all["total"], airline_all["cash_total"], airline_all["card_total"]])
        rows.append([])

    if filters["include_airport"]:
        rows.append(["Airport Service Fees"])
        rows.append(["Airline", "Destination", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"])
        for r in airport_items_summary:
            airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
            destination = _destination_label(r)
            rows.append(
                [
                    airline,
                    destination,
                    r["fee_key"],
                    r["fee_name"],
                    r["qty"],
                    r["total"],
                    r["cash_total"],
                    r["card_total"],
                ]
            )
        rows.append([])
        rows.append(["Airport Fees Totals by Airline"])
        rows.append(["Airline", "Total", "Cash", "Card"])
        for r in airport_totals:
            airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
            rows.append([airline, r["total"], r["cash_total"], r["card_total"]])
        rows.append(["Airport Fees Total (All)"])
        rows.append(["Total", "Cash", "Card"])
        rows.append([airport_all["total"], airport_all["cash_total"], airport_all["card_total"]])
        rows.append([])

    rows.append(["All Fees Total"])
    rows.append(["Total", "Cash", "Card"])
    rows.append([combined["total"], combined["cash_total"], combined["card_total"]])

    if fmt.lower() == "csv":
        flat_rows = []
        flat_rows.append(
            ["Section", "Airline", "Destination", "Item Key", "Item Name", "Qty", "Total", "Cash", "Card"]
        )
        if filters["include_airline"]:
            for r in airline_items_summary:
                airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
                destination = _destination_label(r) if filters.get("destination_ids") else ""
                flat_rows.append(
                    [
                        "Airline Fees",
                        airline,
                        destination,
                        r["fee_key"],
                        r["fee_name"],
                        r["qty"],
                        r["total"],
                        r["cash_total"],
                        r["card_total"],
                    ]
                )
        if filters["include_airport"]:
            for r in airport_items_summary:
                airline = f"{r['name']}{' (' + r['code'] + ')' if r['code'] else ''}"
                destination = _destination_label(r) if filters.get("destination_ids") else ""
                flat_rows.append(
                    [
                        "Airport Fees",
                        airline,
                        destination,
                        r["fee_key"],
                        r["fee_name"],
                        r["qty"],
                        r["total"],
                        r["cash_total"],
                        r["card_total"],
                    ]
                )

        content = _report_to_csv(flat_rows)
        resp = make_response(content)
        resp.headers["Content-Type"] = "text/csv"
        resp.headers["Content-Disposition"] = (
            f"attachment; filename=[CUSTOM REPORT] {filters['date_from']}_to_{filters['date_to']}.csv"
        )
        return resp

    if fmt.lower() == "pdf":
        title = f"Custom Report {filters['date_from']} to {filters['date_to']}"
        content = _custom_report_to_pdf(title, rows, chart_data, filters["date_from"], filters["date_to"])
        resp = make_response(content)
        resp.headers["Content-Type"] = "application/pdf"
        resp.headers["Content-Disposition"] = (
            f"attachment; filename=[CUSTOM REPORT] {filters['date_from']}_to_{filters['date_to']}.pdf"
        )
        return resp

    abort(400)


@app.get("/variable_rewards", endpoint="variable_rewards")
@admin_required
def variable_rewards():
    month_raw = _sanitize(request.args.get("month"))
    year_raw = _sanitize(request.args.get("year"))
    try:
        selected_month = int(month_raw) if month_raw else datetime.now(timezone.utc).month
    except ValueError:
        selected_month = datetime.now(timezone.utc).month
    selected_month = min(12, max(1, selected_month))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fullname, nickname, role, active "
            "FROM users ORDER BY fullname COLLATE NOCASE ASC"
        )
        users_list = cur.fetchall()
        monthly_total = _compute_monthly_airport_total(year, selected_month)
    percent_key = f"variable_rewards_percent_{year}_{selected_month:02d}"
    percent_raw = get_app_state(percent_key) or "100"
    try:
        percent_value = float(percent_raw)
    except ValueError:
        percent_value = 100.0
    percent_value = round(min(100.0, max(0.0, percent_value)))
    manual_amounts = {}
    for u in users_list:
        key = f"variable_rewards_manual_{year}_{selected_month:02d}_{u['id']}"
        raw = get_app_state(key)
        if raw is None:
            continue
        try:
            manual_amounts[str(u["id"])] = float(raw)
        except ValueError:
            continue
    return render_template(
        "variable_rewards.html",
        users=users_list,
        monthly_total=monthly_total,
        selected_month=selected_month,
        selected_year=year,
        percent_value=percent_value,
        manual_amounts=manual_amounts,
        year_options=[year - 2, year - 1, year, year + 1],
    )


@app.get("/account_settings", endpoint="account_settings")
@admin_required
def account_settings():
    smtp = {
        "host": get_app_state("smtp_host") or "",
        "port": get_app_state("smtp_port") or "587",
        "user": get_app_state("smtp_user") or "",
        "sender": get_app_state("smtp_sender") or "",
        "tls": get_app_state("smtp_tls") or "1",
    }
    return render_template("account_settings.html", smtp=smtp)


@app.post("/account_settings/smtp", endpoint="account_settings_smtp")
@admin_required
def account_settings_smtp():
    require_csrf()
    host = _sanitize(request.form.get("smtp_host"))
    port = _sanitize(request.form.get("smtp_port")) or "587"
    user = _sanitize(request.form.get("smtp_user"))
    password = _sanitize(request.form.get("smtp_password"))
    sender = _sanitize(request.form.get("smtp_sender"))
    tls = "1" if request.form.get("smtp_tls") == "on" else "0"

    set_app_state("smtp_host", host)
    set_app_state("smtp_port", port)
    set_app_state("smtp_user", user)
    if password:
        set_app_state("smtp_password", password)
    set_app_state("smtp_sender", sender)
    set_app_state("smtp_tls", tls)

    flash("SMTP settings saved.")
    return redirect(url_for("account_settings"))


@app.route("/notifications", methods=["GET", "POST"], endpoint="notifications")
@admin_required
def notifications():
    if request.method == "POST":
        require_csrf()
        emails = []
        seen = set()
        for i in range(1, 11):
            raw = _sanitize(request.form.get(f"email_{i}"))
            if not raw:
                continue
            email = raw.lower()
            if not _is_valid_email(email):
                with get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT id, name, slug, subject, body, enabled "
                        "FROM notification_templates ORDER BY id ASC"
                    )
                    templates = cur.fetchall()
                flash(f"Invalid email: {raw}")
                return render_template("notifications.html", emails=request.form, templates=templates)
            if email in seen:
                continue
            seen.add(email)
            emails.append(email)

        now = datetime.now(timezone.utc).isoformat()
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM notification_emails")
            for email in emails:
                cur.execute(
                    "INSERT INTO notification_emails (email, created_at_utc) VALUES (?, ?)",
                    (email, now),
                )
            conn.commit()
        flash("Notifications saved.")
        return redirect(url_for("notifications"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT email FROM notification_emails ORDER BY id ASC")
        rows = cur.fetchall()
        cur.execute(
            "SELECT id, name, slug, subject, body, enabled FROM notification_templates ORDER BY id ASC"
        )
        templates = cur.fetchall()
    emails = [r["email"] for r in rows]
    while len(emails) < 10:
        emails.append("")
    return render_template("notifications.html", emails=emails, templates=templates)


@app.post("/notifications/templates", endpoint="notification_template_create")
@admin_required
def notification_template_create():
    require_csrf()
    name = _sanitize(request.form.get("name"))
    subject = _sanitize(request.form.get("subject"))
    body = _sanitize(request.form.get("body"))
    enabled = 1 if request.form.get("enabled") == "on" else 0
    if not (name and subject and body):
        flash("Please fill name, subject and body.")
        return redirect(url_for("notifications"))

    slug_base = _slugify(name)
    slug = slug_base
    with get_connection() as conn:
        cur = conn.cursor()
        idx = 2
        while True:
            cur.execute("SELECT 1 FROM notification_templates WHERE slug = ?", (slug,))
            if not cur.fetchone():
                break
            slug = f"{slug_base}_{idx}"
            idx += 1
        now = _utc_now_iso()
        cur.execute(
            """
            INSERT INTO notification_templates (name, slug, subject, body, enabled, created_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (name, slug, subject, body, enabled, now, now),
        )
        conn.commit()
    flash("Notification template created.")
    return redirect(url_for("notifications"))


@app.post("/notifications/templates/<int:template_id>", endpoint="notification_template_update")
@admin_required
def notification_template_update(template_id: int):
    require_csrf()
    name = _sanitize(request.form.get("name"))
    subject = _sanitize(request.form.get("subject"))
    body = _sanitize(request.form.get("body"))
    enabled = 1 if request.form.get("enabled") == "on" else 0
    if not (name and subject and body):
        flash("Please fill name, subject and body.")
        return redirect(url_for("notifications"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE notification_templates
            SET name = ?, subject = ?, body = ?, enabled = ?, updated_at_utc = ?
            WHERE id = ?
            """,
            (name, subject, body, enabled, _utc_now_iso(), template_id),
        )
        conn.commit()
    flash("Notification template updated.")
    return redirect(url_for("notifications"))


# -----------------------------------------------------------------------------
# Users management (Admin + Deputy for approval; edit/delete are Admin only)
# -----------------------------------------------------------------------------
@app.get("/users", endpoint="users")
@approver_required
def users():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, fullname, nickname, role, approved, created_at_utc, approved_at_utc "
            "FROM users ORDER BY approved ASC, id ASC"
        )
        all_users = cur.fetchall()
    return render_template("users.html", users=all_users)


@app.post("/users/<int:user_id>/approve", endpoint="approve_user")
@approver_required
def approve_user(user_id: int):
    require_csrf()

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, approved FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            flash("User not found.")
            return redirect(url_for("users"))

        if int(row["approved"]) == 1:
            flash("User is already approved.")
            return redirect(url_for("users"))

        cur.execute(
            """
            UPDATE users
            SET approved = 1,
                approved_by = ?,
                approved_at_utc = ?
            WHERE id = ?
            """,
            (session.get("user_id"), _utc_now_iso(), user_id),
        )
        conn.commit()

    flash("✅ User approved.")
    return redirect(url_for("users"))


def _count_admins() -> int:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM users WHERE role = 'Admin'")
        row = cur.fetchone()
    return int(row["c"] if row else 0)


@app.route("/users/<int:user_id>/edit", methods=["GET", "POST"], endpoint="edit_user")
@admin_required
def edit_user(user_id: int):
    if request.method == "GET":
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, fullname, nickname, role FROM users WHERE id = ?", (user_id,))
            user = cur.fetchone()
        if not user:
            flash("User not found.")
            return redirect(url_for("users"))
        return render_template("edit_users.html", user=user)

    require_csrf()
    fullname = _sanitize(request.form.get("fullname"))
    nickname = _sanitize(request.form.get("nickname"))
    role = _sanitize(request.form.get("role")) or "User"

    if role not in {"User", "Admin", "Deputy"}:
        flash("Invalid role.")
        return redirect(url_for("edit_user", user_id=user_id))

    if not fullname or not nickname:
        flash("Full name and nickname are required.")
        return redirect(url_for("edit_user", user_id=user_id))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT role FROM users WHERE id = ?", (user_id,))
        current = cur.fetchone()
        if not current:
            flash("User not found.")
            return redirect(url_for("users"))

        if current["role"] == "Admin" and role != "Admin" and _count_admins() <= 1:
            flash("You cannot remove the last Admin. Reassign Admin role first.")
            return redirect(url_for("reassign_admin"))

        try:
            cur.execute(
                "UPDATE users SET fullname = ?, nickname = ?, role = ? WHERE id = ?",
                (fullname, nickname, role, user_id),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            flash("Nickname already exists.")
            return redirect(url_for("edit_user", user_id=user_id))

    flash("User updated.")
    return redirect(url_for("users"))


@app.post("/users/<int:user_id>/delete", endpoint="delete_user")
@admin_required
def delete_user(user_id: int):
    require_csrf()

    if session.get("user_id") == user_id:
        flash("You cannot delete the currently logged-in user.")
        return redirect(url_for("users"))

    deleted_name = None
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT role, fullname FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            flash("User not found.")
            return redirect(url_for("users"))

        deleted_name = row["fullname"]
        if row["role"] == "Admin" and _count_admins() <= 1:
            flash("You cannot delete the last Admin. Reassign Admin role first.")
            return redirect(url_for("reassign_admin"))

        cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()

    if deleted_name:
        try:
            send_notification("user_deleted", {"UserName": deleted_name})
        except Exception:
            pass

    flash("User deleted.")
    return redirect(url_for("users"))


@app.post("/users/<int:user_id>/reset_password", endpoint="reset_user_password")
@admin_required
def reset_user_password(user_id: int):
    require_csrf()

    if session.get("user_id") == user_id:
        flash("You cannot reset the currently logged-in user.")
        return redirect(url_for("users"))

    temp_password = _generate_temp_password()

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            flash("User not found.")
            return redirect(url_for("users"))

        cur.execute(
            "UPDATE users SET password = ?, must_change_password = 1 WHERE id = ?",
            (hash_password(temp_password), user_id),
        )
        conn.commit()

    flash(f"Temporary password: {temp_password} (user must change it on next login)")
    return redirect(url_for("users"))


@app.route(
    "/users/<int:user_id>/reset_questions",
    methods=["GET", "POST"],
    endpoint="reset_user_questions",
)
@admin_required
def reset_user_questions(user_id: int):
    if request.method == "GET":
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, fullname, nickname FROM users WHERE id = ?", (user_id,))
            user = cur.fetchone()
        if not user:
            flash("User not found.")
            return redirect(url_for("users"))
        return render_template("reset_questions.html", user=user)

    require_csrf()
    q1 = _sanitize(request.form.get("q1"))
    a1 = _sanitize(request.form.get("a1"))
    q2 = _sanitize(request.form.get("q2"))
    a2 = _sanitize(request.form.get("a2"))
    q3 = _sanitize(request.form.get("q3"))
    a3 = _sanitize(request.form.get("a3"))

    if not (q1 and a1 and q2 and a2 and q3 and a3):
        flash("All questions and answers are required.")
        return redirect(url_for("reset_user_questions", user_id=user_id))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            flash("User not found.")
            return redirect(url_for("users"))

        cur.execute(
            "UPDATE users SET q1 = ?, a1 = ?, q2 = ?, a2 = ?, q3 = ?, a3 = ? WHERE id = ?",
            (q1, a1, q2, a2, q3, a3, user_id),
        )
        conn.commit()

    flash("Security questions updated.")
    return redirect(url_for("users"))


@app.get("/users/<int:user_id>/logs", endpoint="user_logs")
@admin_required
def user_logs(user_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, fullname, nickname, role FROM users WHERE id = ?", (user_id,))
        user = cur.fetchone()
        if not user:
            flash("User not found.")
            return redirect(url_for("users"))

        cur.execute(
            """
            SELECT action, success, ip, user_agent, details, created_at_utc
            FROM auth_logs
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (user_id,),
        )
        logs = cur.fetchall()

        cur.execute(
            """
            SELECT action, sale_id, ip, user_agent, details, created_at_utc
            FROM sales_logs
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (user_id,),
        )
        sales_logs = cur.fetchall()

    return render_template("user_logs.html", user=user, logs=logs, sales_logs=sales_logs)


@app.route("/reassign_admin", methods=["GET", "POST"], endpoint="reassign_admin")
@admin_required
def reassign_admin():
    if request.method == "GET":
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id, fullname FROM users WHERE role != 'Admin' ORDER BY fullname ASC")
            candidates = cur.fetchall()
        if not candidates:
            flash("No non-admin users available to promote.")
            return redirect(url_for("users"))
        return render_template("reassign_admin.html", users=candidates)

    require_csrf()
    admin_id_raw = request.form.get("admin_id") or ""
    try:
        admin_id = int(admin_id_raw)
    except ValueError:
        flash("Invalid selection.")
        return redirect(url_for("reassign_admin"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE id = ?", (admin_id,))
        target = cur.fetchone()
        if not target:
            flash("Selected user not found.")
            return redirect(url_for("reassign_admin"))

        cur.execute("UPDATE users SET role = 'Admin' WHERE id = ?", (admin_id,))
        conn.commit()

    flash("Admin role reassigned.")
    return redirect(url_for("users"))


# -----------------------------------------------------------------------------
# Airlines CRUD + Fees management (Admin only)
# -----------------------------------------------------------------------------
def _parse_bool_checkbox(value: str | None) -> int:
    return 1 if value in {"on", "true", "1", "yes"} else 0


def _parse_amount(value: str | None) -> float:
    try:
        return float((value or "").replace(",", "."))
    except ValueError:
        return 0.0


@app.get("/airlines", endpoint="airlines")
@admin_required
def airlines():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, code, country, active, created_at_utc, updated_at_utc "
            "FROM airlines ORDER BY name COLLATE NOCASE ASC"
        )
        items = cur.fetchall()
    return render_template("airlines.html", airlines=items)


@app.route("/airlines/add", methods=["GET", "POST"], endpoint="airlines_add")
@admin_required
def airlines_add():
    if request.method == "GET":
        return render_template("airline_add.html")

    require_csrf()
    name = _sanitize(request.form.get("name"))
    code = _sanitize(request.form.get("code"))
    country = _sanitize(request.form.get("country"))
    active = _parse_bool_checkbox(request.form.get("active"))
    now = _utc_now_iso()

    if not name:
        flash("Name is required.")
        return redirect(url_for("airlines_add"))

    with get_connection() as conn:
        cur = conn.cursor()

        if code:
            cur.execute("SELECT 1 FROM airlines WHERE code = ?", (code,))
            if cur.fetchone():
                flash("Airline code must be unique.")
                return redirect(url_for("airlines_add"))

        cur.execute(
            """
            INSERT INTO airlines (name, code, country, active, created_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (name, code or None, country or None, active, now, now),
        )
        conn.commit()

    flash("Airline created.")
    return redirect(url_for("airlines"))


@app.get("/airlines/<int:airline_id>", endpoint="airline_detail")
@admin_required
def airline_detail(airline_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, code, country, active, created_at_utc, updated_at_utc "
            "FROM airlines WHERE id = ?",
            (airline_id,),
        )
        airline = cur.fetchone()
    if not airline:
        flash("Airline not found.")
        return redirect(url_for("airlines"))
    return render_template("airline_detail.html", airline=airline)


@app.get("/airport_service_fees", endpoint="airport_service_fees")
@admin_required
def airport_service_fees():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, fee_key, fee_name, amount, currency, unit, notes, updated_at_utc
            FROM airport_service_fees
            ORDER BY fee_key COLLATE NOCASE ASC
            """
        )
        fees = cur.fetchall()
    return render_template("airport_service_fees.html", fees=fees)


@app.post("/airport_service_fees/add", endpoint="airport_service_fees_add")
@admin_required
def airport_service_fees_add():
    require_csrf()
    fee_key = _sanitize(request.form.get("fee_key"))
    fee_name = _sanitize(request.form.get("fee_name"))
    amount = _parse_amount(request.form.get("amount"))
    currency = _sanitize(request.form.get("currency")) or "EUR"
    unit = _sanitize(request.form.get("unit"))
    notes = _sanitize(request.form.get("notes"))
    now = _utc_now_iso()

    if not fee_key or not fee_name:
        flash("Fee key and name are required.")
        return redirect(url_for("airport_service_fees"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM airport_service_fees WHERE fee_key = ?", (fee_key,))
        if cur.fetchone():
            flash("Fee key must be unique.")
            return redirect(url_for("airport_service_fees"))

        cur.execute(
            """
            INSERT INTO airport_service_fees
                (fee_key, fee_name, amount, currency, unit, notes, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (fee_key, fee_name, amount, currency, unit or None, notes or None, now),
        )
        conn.commit()

    flash("Fee added.")
    return redirect(url_for("airport_service_fees"))


@app.route(
    "/airport_service_fees/<int:fee_id>/edit",
    methods=["GET", "POST"],
    endpoint="airport_service_fee_edit",
)
@admin_required
def airport_service_fee_edit(fee_id: int):
    if request.method == "GET":
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, fee_key, fee_name, amount, currency, unit, notes
                FROM airport_service_fees
                WHERE id = ?
                """,
                (fee_id,),
            )
            fee = cur.fetchone()
        if not fee:
            flash("Fee not found.")
            return redirect(url_for("airport_service_fees"))
        return render_template("airport_service_fee_edit.html", fee=fee)

    require_csrf()
    fee_key = _sanitize(request.form.get("fee_key"))
    fee_name = _sanitize(request.form.get("fee_name"))
    amount = _parse_amount(request.form.get("amount"))
    currency = _sanitize(request.form.get("currency")) or "EUR"
    unit = _sanitize(request.form.get("unit"))
    notes = _sanitize(request.form.get("notes"))
    now = _utc_now_iso()

    if not fee_key or not fee_name:
        flash("Fee key and name are required.")
        return redirect(url_for("airport_service_fee_edit", fee_id=fee_id))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM airport_service_fees WHERE fee_key = ? AND id != ?",
            (fee_key, fee_id),
        )
        if cur.fetchone():
            flash("Fee key must be unique.")
            return redirect(url_for("airport_service_fee_edit", fee_id=fee_id))

        cur.execute(
            """
            UPDATE airport_service_fees
            SET fee_key = ?, fee_name = ?, amount = ?, currency = ?, unit = ?, notes = ?, updated_at_utc = ?
            WHERE id = ?
            """,
            (fee_key, fee_name, amount, currency, unit or None, notes or None, now, fee_id),
        )
        conn.commit()

    flash("Fee updated.")
    return redirect(url_for("airport_service_fees"))


@app.post("/airport_service_fees/<int:fee_id>/delete", endpoint="airport_service_fee_delete")
@admin_required
def airport_service_fee_delete(fee_id: int):
    require_csrf()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM airport_service_fees WHERE id = ?", (fee_id,))
        conn.commit()
    flash("Fee deleted.")
    return redirect(url_for("airport_service_fees"))

@app.post("/variable_rewards/<int:user_id>/active", endpoint="variable_rewards_active")
@admin_required
def variable_rewards_active(user_id: int):
    require_csrf()
    active = _parse_bool_checkbox(request.form.get("active"))
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM users WHERE id = ?", (user_id,))
        if not cur.fetchone():
            flash("User not found.")
            return redirect(url_for("variable_rewards"))
        cur.execute("UPDATE users SET active = ? WHERE id = ?", (active, user_id))
        conn.commit()
    flash("Active status updated.")
    return redirect(url_for("variable_rewards"))


@app.post("/variable_rewards/percent", endpoint="variable_rewards_percent")
@admin_required
def variable_rewards_percent():
    require_csrf()
    percent_value = _parse_amount(request.form.get("percent_value"))
    percent_value = round(min(100.0, max(0.0, percent_value)))
    month_raw = _sanitize(request.form.get("month"))
    year_raw = _sanitize(request.form.get("year"))
    try:
        month = int(month_raw) if month_raw else datetime.now(timezone.utc).month
    except ValueError:
        month = datetime.now(timezone.utc).month
    month = min(12, max(1, month))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    percent_key = f"variable_rewards_percent_{year}_{month:02d}"
    set_app_state(percent_key, str(int(percent_value)))
    if month_raw:
        return redirect(url_for("variable_rewards", month=month, year=year))
    return redirect(url_for("variable_rewards", year=year))


@app.post("/variable_rewards/manual/<int:user_id>", endpoint="variable_rewards_manual")
@admin_required
def variable_rewards_manual(user_id: int):
    require_csrf()
    amount = _parse_amount(request.form.get("manual_amount"))
    amount = max(0.0, amount)
    month_raw = _sanitize(request.form.get("month"))
    year_raw = _sanitize(request.form.get("year"))
    try:
        month = int(month_raw) if month_raw else datetime.now(timezone.utc).month
    except ValueError:
        month = datetime.now(timezone.utc).month
    month = min(12, max(1, month))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    key = f"variable_rewards_manual_{year}_{month:02d}_{user_id}"
    set_app_state(key, str(amount))
    flash("Manual amount saved.")
    return redirect(url_for("variable_rewards", month=month, year=year))


@app.post("/variable_rewards/save", endpoint="variable_rewards_save")
@admin_required
def variable_rewards_save():
    require_csrf()
    month_raw = _sanitize(request.form.get("month"))
    year_raw = _sanitize(request.form.get("year"))
    try:
        month = int(month_raw) if month_raw else datetime.now(timezone.utc).month
    except ValueError:
        month = datetime.now(timezone.utc).month
    month = min(12, max(1, month))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year

    monthly_total, percent_value, reduced_total, computed_users = _compute_variable_rewards_distribution(
        year, month
    )
    _save_variable_rewards_snapshot(
        year, month, monthly_total, percent_value, reduced_total, computed_users
    )

    flash("Snapshot saved.")
    return redirect(url_for("variable_rewards", month=month, year=year))


@app.get("/variable_rewards/summary", endpoint="variable_rewards_summary")
@admin_required
def variable_rewards_summary():
    year_raw = _sanitize(request.args.get("year"))
    month_from_raw = _sanitize(request.args.get("month_from"))
    month_to_raw = _sanitize(request.args.get("month_to"))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    try:
        month_from = int(month_from_raw) if month_from_raw else 1
    except ValueError:
        month_from = 1
    try:
        month_to = int(month_to_raw) if month_to_raw else datetime.now(timezone.utc).month
    except ValueError:
        month_to = datetime.now(timezone.utc).month
    month_from = min(12, max(1, month_from))
    month_to = min(12, max(1, month_to))
    if month_to < month_from:
        month_from, month_to = month_to, month_from
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.id, u.fullname, u.nickname, u.role,
                   COALESCE(SUM(s.computed_amount), 0) AS computed_amount
            FROM users u
            LEFT JOIN variable_rewards_snapshots s
              ON s.user_id = u.id
             AND s.year = ?
             AND s.scope = 'monthly'
             AND s.month BETWEEN ? AND ?
            GROUP BY u.id
            ORDER BY u.fullname COLLATE NOCASE ASC
            """,
            (year, month_from, month_to),
        )
        rows = cur.fetchall()
        cur.execute(
            """
            SELECT SUM(month_total) AS total
            FROM (
                SELECT month, MAX(reduced_total) AS month_total
                FROM variable_rewards_snapshots
                WHERE year = ? AND scope = 'monthly' AND month BETWEEN ? AND ?
                GROUP BY month
            ) t
            """,
            (year, month_from, month_to),
        )
        total_row = cur.fetchone()
        total_reduced = float(total_row["total"] or 0)
    return render_template(
        "variable_rewards_summary.html",
        year=year,
        month_from=month_from,
        month_to=month_to,
        rows=rows,
        total_reduced=total_reduced,
    )


@app.get("/variable_rewards/summary/pdf", endpoint="variable_rewards_summary_pdf")
@admin_required
def variable_rewards_summary_pdf():
    year_raw = _sanitize(request.args.get("year"))
    month_from_raw = _sanitize(request.args.get("month_from"))
    month_to_raw = _sanitize(request.args.get("month_to"))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    try:
        month_from = int(month_from_raw) if month_from_raw else 1
    except ValueError:
        month_from = 1
    try:
        month_to = int(month_to_raw) if month_to_raw else datetime.now(timezone.utc).month
    except ValueError:
        month_to = datetime.now(timezone.utc).month
    month_from = min(12, max(1, month_from))
    month_to = min(12, max(1, month_to))
    if month_to < month_from:
        month_from, month_to = month_to, month_from
    month_names = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.fullname, u.nickname, u.role,
                   COALESCE(SUM(s.computed_amount), 0) AS computed_amount
            FROM users u
            LEFT JOIN variable_rewards_snapshots s
              ON s.user_id = u.id
             AND s.year = ?
             AND s.scope = 'monthly'
             AND s.month BETWEEN ? AND ?
            GROUP BY u.id
            ORDER BY u.fullname COLLATE NOCASE ASC
            """,
            (year, month_from, month_to),
        )
        rows_db = cur.fetchall()
        cur.execute(
            """
            SELECT SUM(month_total) AS total
            FROM (
                SELECT month, MAX(reduced_total) AS month_total
                FROM variable_rewards_snapshots
                WHERE year = ? AND scope = 'monthly' AND month BETWEEN ? AND ?
                GROUP BY month
            ) t
            """,
            (year, month_from, month_to),
        )
        total_row = cur.fetchone()
        total_reduced = float(total_row["total"] or 0)

    rows = []
    title = "Yearly Rewards Summary"
    report_range = f"{month_names[month_from]}-{month_names[month_to]} {year}"
    rows.append([f"{title} ({report_range})"])
    rows.append(["#", "Full name", "Nickname", "Role", "Total (Range)"])
    for idx, r in enumerate(rows_db, start=1):
        rows.append(
            [
                idx,
                r["fullname"],
                r["nickname"],
                r["role"],
                f"{float(r['computed_amount'] or 0):.2f} EUR",
            ]
        )
    rows.append([])
    rows.append(["Total reduced reward paid (Range)"])
    rows.append(["Amount"])
    rows.append([f"{total_reduced:.2f} EUR"])

    report_title = f"{title} {report_range}"
    content = _report_to_pdf(report_title, rows)
    resp = make_response(content)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = (
        f"attachment; filename=[REWARDS SUMMARY] {year}-{month_from:02d}-{month_to:02d}.pdf"
    )
    return resp


@app.get("/variable_rewards/summary/print/<int:user_id>", endpoint="variable_rewards_summary_print_user")
@admin_required
def variable_rewards_summary_print_user(user_id: int):
    year_raw = _sanitize(request.args.get("year"))
    month_from_raw = _sanitize(request.args.get("month_from"))
    month_to_raw = _sanitize(request.args.get("month_to"))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year
    try:
        month_from = int(month_from_raw) if month_from_raw else 1
    except ValueError:
        month_from = 1
    try:
        month_to = int(month_to_raw) if month_to_raw else datetime.now(timezone.utc).month
    except ValueError:
        month_to = datetime.now(timezone.utc).month
    month_from = min(12, max(1, month_from))
    month_to = min(12, max(1, month_to))
    if month_to < month_from:
        month_from, month_to = month_to, month_from

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT fullname, nickname FROM users WHERE id = ?", (user_id,))
        user = cur.fetchone()
        if not user:
            flash("User not found.")
            return redirect(url_for("variable_rewards_summary", year=year, month_from=month_from, month_to=month_to))

        cur.execute(
            """
            SELECT month, computed_amount
            FROM variable_rewards_snapshots
            WHERE year = ? AND scope = 'monthly' AND user_id = ? AND month BETWEEN ? AND ?
            ORDER BY month ASC
            """,
            (year, user_id, month_from, month_to),
        )
        month_rows = {int(r["month"]): float(r["computed_amount"] or 0) for r in cur.fetchall()}

    month_names = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    rows = []
    title = f"Yearly Summary - {user['fullname'] or user['nickname']}"
    period = f"{month_names[month_from]}-{month_names[month_to]} {year}"
    rows.append(["Monthly breakdown"])
    rows.append(["Month", "Amount", "Total"])
    total = 0.0
    for m in range(month_from, month_to + 1):
        amount = float(month_rows.get(m, 0.0))
        total += amount
        rows.append(
            [
                f"{month_names[m]} {year}",
                f"{amount:.2f} EUR",
                f"{total:.2f} EUR",
            ]
        )
    rows.append([])
    rows.append(["Total"])
    rows.append([f"{total:.2f} EUR"])

    report_title = f"{title} ({period})"
    content = _report_to_pdf(report_title, rows)
    resp = make_response(content)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = (
        f"attachment; filename=[SUMMARY] {user['fullname'] or user['nickname']} {year}-{month_from:02d}-{month_to:02d}.pdf"
    )
    return resp


@app.get("/variable_rewards/print_all", endpoint="variable_rewards_print_all")
@admin_required
def variable_rewards_print_all():
    month_raw = _sanitize(request.args.get("month"))
    year_raw = _sanitize(request.args.get("year"))
    try:
        month = int(month_raw) if month_raw else datetime.now(timezone.utc).month
    except ValueError:
        month = datetime.now(timezone.utc).month
    month = min(12, max(1, month))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year

    monthly_total, percent_value, reduced_total, computed_users = _compute_variable_rewards_distribution(
        year, month
    )
    _save_variable_rewards_snapshot(
        year, month, monthly_total, percent_value, reduced_total, computed_users
    )

    month_names = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    rows = []
    title = "Variable Rewards"
    period = f"{month_names[month]} {year}"
    rows.append([f"{title} ({period})"])
    rows.append(["#", "Full name", "Nickname", "Role", "Total"])
    for idx, u in enumerate(computed_users, start=1):
        rows.append(
            [
                idx,
                u["fullname"],
                u["nickname"],
                u["role"],
                f"{float(u['computed_amount'] or 0):.2f} EUR",
            ]
        )
    rows.append([])
    rows.append(["Total reduced reward paid"])
    rows.append(["Amount"])
    rows.append([f"{reduced_total:.2f} EUR"])

    report_title = f"{title} {period}"
    content = _report_to_pdf(report_title, rows)
    resp = make_response(content)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = (
        f"attachment; filename=[VARIABLE REWARDS] {year}-{month:02d}.pdf"
    )
    return resp


@app.get("/variable_rewards/print/<int:user_id>", endpoint="variable_rewards_print")
@admin_required
def variable_rewards_print(user_id: int):
    month_raw = _sanitize(request.args.get("month"))
    year_raw = _sanitize(request.args.get("year"))
    try:
        month = int(month_raw) if month_raw else datetime.now(timezone.utc).month
    except ValueError:
        month = datetime.now(timezone.utc).month
    month = min(12, max(1, month))
    try:
        year = int(year_raw) if year_raw else datetime.now(timezone.utc).year
    except ValueError:
        year = datetime.now(timezone.utc).year

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT fullname, nickname FROM users WHERE id = ?", (user_id,))
        user = cur.fetchone()
        if not user:
            flash("User not found.")
            return redirect(url_for("variable_rewards", month=month, year=year))

        cur.execute(
            """
            SELECT month, computed_amount
            FROM variable_rewards_snapshots
            WHERE year = ? AND scope = 'monthly' AND user_id = ? AND month BETWEEN 1 AND ?
            ORDER BY month ASC
            """,
            (year, user_id, month),
        )
        month_rows = cur.fetchall()

        cur.execute(
            """
            SELECT computed_amount
            FROM variable_rewards_snapshots
            WHERE year = ? AND scope = 'yearly' AND user_id = ? AND month = ?
            """,
            (year, user_id, month),
        )
        ytd_row = cur.fetchone()

    month_names = [
        "", "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ]
    rows = []
    title = f"Variable Rewards - {user['fullname'] or user['nickname']}"
    period_label = f"{month_names[month]} {year}"
    rows.append([title, f"Up to {period_label}"])
    rows.append([])
    rows.append(["Month", "Amount"])
    for r in month_rows:
        m = int(r["month"] or 0)
        label = f"{month_names[m]} {year}" if 1 <= m <= 12 else str(m)
        rows.append([label, f"{float(r['computed_amount'] or 0):.2f}"])
    rows.append([])
    rows.append(["Year-to-date total"])
    if ytd_row and ytd_row["computed_amount"] is not None:
        ytd_total = float(ytd_row["computed_amount"] or 0)
    else:
        ytd_total = sum(float(r["computed_amount"] or 0) for r in month_rows)
    rows.append([f"{ytd_total:.2f}"])

    content = _report_to_pdf(f"{title} ({period_label})", rows)
    resp = make_response(content)
    resp.headers["Content-Type"] = "application/pdf"
    resp.headers["Content-Disposition"] = (
        f"attachment; filename=[REWARDS] {user['fullname'] or user['nickname']} {year}-{month:02d}.pdf"
    )
    return resp
@app.route("/airlines/<int:airline_id>/edit", methods=["GET", "POST"], endpoint="airlines_edit")
@admin_required
def airlines_edit(airline_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, code, country, active FROM airlines WHERE id = ?", (airline_id,))
        airline = cur.fetchone()

    if not airline:
        flash("Airline not found.")
        return redirect(url_for("airlines"))

    if request.method == "GET":
        return render_template("airline_edit.html", airline=airline)

    require_csrf()
    name = _sanitize(request.form.get("name"))
    code = _sanitize(request.form.get("code"))
    country = _sanitize(request.form.get("country"))
    active = _parse_bool_checkbox(request.form.get("active"))
    now = _utc_now_iso()

    if not name:
        flash("Name is required.")
        return redirect(url_for("airlines_edit", airline_id=airline_id))

    with get_connection() as conn:
        cur = conn.cursor()

        if code:
            cur.execute("SELECT 1 FROM airlines WHERE code = ? AND id != ?", (code, airline_id))
            if cur.fetchone():
                flash("Airline code must be unique.")
                return redirect(url_for("airlines_edit", airline_id=airline_id))

        cur.execute(
            """
            UPDATE airlines
            SET name = ?, code = ?, country = ?, active = ?, updated_at_utc = ?
            WHERE id = ?
            """,
            (name, code or None, country or None, active, now, airline_id),
        )
        conn.commit()

    flash("Airline updated.")
    return redirect(url_for("airline_detail", airline_id=airline_id))


@app.post("/airlines/<int:airline_id>/delete", endpoint="airlines_delete")
@admin_required
def airlines_delete(airline_id: int):
    require_csrf()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM airlines WHERE id = ?", (airline_id,))
        conn.commit()
    flash("Airline deleted.")
    return redirect(url_for("airlines"))


@app.get("/fees/select", endpoint="fees_select")
@admin_required
def fees_select():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, code FROM airlines WHERE active = 1 ORDER BY name COLLATE NOCASE ASC")
        airlines_list = cur.fetchall()
    return render_template("fees_select.html", airlines=airlines_list)


@app.get("/airlines/<int:airline_id>/fees", endpoint="airline_fees")
@admin_required
def airline_fees(airline_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, code FROM airlines WHERE id = ?", (airline_id,))
        airline = cur.fetchone()
        if not airline:
            flash("Airline not found.")
            return redirect(url_for("fees_select"))

        cur.execute(
            """
            SELECT id, fee_key, fee_name, amount, currency, unit, notes, updated_at_utc
            FROM airline_fees
            WHERE airline_id = ?
            ORDER BY fee_name COLLATE NOCASE ASC
            """,
            (airline_id,),
        )
        fees = cur.fetchall()

    return render_template("airline_fees.html", airline=airline, fees=fees)


@app.post("/airlines/<int:airline_id>/fees/add", endpoint="airline_fees_add")
@admin_required
def airline_fees_add(airline_id: int):
    require_csrf()
    fee_key = _sanitize(request.form.get("fee_key")).upper()
    fee_name = _sanitize(request.form.get("fee_name"))
    amount = _parse_amount(request.form.get("amount"))
    currency = _sanitize(request.form.get("currency")) or "EUR"
    unit = _sanitize(request.form.get("unit"))
    notes = _sanitize(request.form.get("notes"))
    now = _utc_now_iso()

    if not fee_key or not fee_name:
        flash("Fee key and fee name are required.")
        return redirect(url_for("airline_fees", airline_id=airline_id))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM airline_fees WHERE airline_id = ? AND fee_key = ?", (airline_id, fee_key))
        if cur.fetchone():
            flash("Fee key must be unique for this airline.")
            return redirect(url_for("airline_fees", airline_id=airline_id))

        cur.execute(
            """
            INSERT INTO airline_fees
                (airline_id, fee_key, fee_name, amount, currency, unit, notes, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (airline_id, fee_key, fee_name, amount, currency, unit or None, notes or None, now),
        )
        conn.commit()

    flash("Fee added.")
    return redirect(url_for("airline_fees", airline_id=airline_id))


@app.route(
    "/airlines/<int:airline_id>/fees/<int:fee_id>/edit",
    methods=["GET", "POST"],
    endpoint="airline_fee_edit",
)
@admin_required
def airline_fee_edit(airline_id: int, fee_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, fee_key, fee_name, amount, currency, unit, notes
            FROM airline_fees
            WHERE id = ? AND airline_id = ?
            """,
            (fee_id, airline_id),
        )
        fee = cur.fetchone()

    if not fee:
        flash("Fee not found.")
        return redirect(url_for("airline_fees", airline_id=airline_id))

    if request.method == "GET":
        return render_template("fee_edit.html", airline_id=airline_id, fee=fee)

    require_csrf()
    fee_key = _sanitize(request.form.get("fee_key")).upper()
    fee_name = _sanitize(request.form.get("fee_name"))
    amount = _parse_amount(request.form.get("amount"))
    currency = _sanitize(request.form.get("currency")) or "EUR"
    unit = _sanitize(request.form.get("unit"))
    notes = _sanitize(request.form.get("notes"))
    now = _utc_now_iso()

    if not fee_key or not fee_name:
        flash("Fee key and fee name are required.")
        return redirect(url_for("airline_fee_edit", airline_id=airline_id, fee_id=fee_id))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM airline_fees WHERE airline_id = ? AND fee_key = ? AND id != ?",
            (airline_id, fee_key, fee_id),
        )
        if cur.fetchone():
            flash("Fee key must be unique for this airline.")
            return redirect(url_for("airline_fee_edit", airline_id=airline_id, fee_id=fee_id))

        cur.execute(
            """
            UPDATE airline_fees
            SET fee_key = ?, fee_name = ?, amount = ?, currency = ?, unit = ?, notes = ?, updated_at_utc = ?
            WHERE id = ? AND airline_id = ?
            """,
            (fee_key, fee_name, amount, currency, unit or None, notes or None, now, fee_id, airline_id),
        )
        conn.commit()

    flash("Fee updated.")
    return redirect(url_for("airline_fees", airline_id=airline_id))


@app.post("/airlines/<int:airline_id>/fees/<int:fee_id>/delete", endpoint="airline_fee_delete")
@admin_required
def airline_fee_delete(airline_id: int, fee_id: int):
    require_csrf()
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM airline_fees WHERE id = ? AND airline_id = ?", (fee_id, airline_id))
        conn.commit()
    flash("Fee deleted.")
    return redirect(url_for("airline_fees", airline_id=airline_id))


@app.get("/airlines/<int:airline_id>/destinations", endpoint="airline_destinations")
@admin_required
def airline_destinations(airline_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, code FROM airlines WHERE id = ?", (airline_id,))
        airline = cur.fetchone()
        if not airline:
            flash("Airline not found.")
            return redirect(url_for("airlines"))
        cur.execute(
            """
            SELECT id, dest_code, dest_name, active, created_at_utc, updated_at_utc
            FROM airline_destinations
            WHERE airline_id = ?
            ORDER BY dest_name COLLATE NOCASE ASC
            """,
            (airline_id,),
        )
        destinations = cur.fetchall()
    return render_template("airline_destinations.html", airline=airline, destinations=destinations)


@app.post("/airlines/<int:airline_id>/destinations/add", endpoint="airline_destinations_add")
@admin_required
def airline_destinations_add(airline_id: int):
    require_csrf()
    dest_code = _sanitize(request.form.get("dest_code")).upper()
    dest_name = _sanitize(request.form.get("dest_name"))
    active = _parse_bool_checkbox(request.form.get("active"))
    now = _utc_now_iso()

    if not dest_name:
        flash("Destination name is required.")
        return redirect(url_for("airline_destinations", airline_id=airline_id))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM airlines WHERE id = ?", (airline_id,))
        if not cur.fetchone():
            flash("Airline not found.")
            return redirect(url_for("airlines"))

        if dest_code:
            cur.execute(
                "SELECT 1 FROM airline_destinations WHERE airline_id = ? AND dest_code = ?",
                (airline_id, dest_code),
            )
            if cur.fetchone():
                flash("Destination code must be unique for this airline.")
                return redirect(url_for("airline_destinations", airline_id=airline_id))

        cur.execute(
            """
            INSERT INTO airline_destinations
                (airline_id, dest_code, dest_name, active, created_at_utc, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (airline_id, dest_code or None, dest_name, active, now, now),
        )
        conn.commit()

    flash("Destination added.")
    return redirect(url_for("airline_destinations", airline_id=airline_id))


@app.route(
    "/airlines/<int:airline_id>/destinations/<int:destination_id>/edit",
    methods=["GET", "POST"],
    endpoint="airline_destination_edit",
)
@admin_required
def airline_destination_edit(airline_id: int, destination_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, dest_code, dest_name, active
            FROM airline_destinations
            WHERE id = ? AND airline_id = ?
            """,
            (destination_id, airline_id),
        )
        destination = cur.fetchone()
    if not destination:
        flash("Destination not found.")
        return redirect(url_for("airline_destinations", airline_id=airline_id))

    if request.method == "GET":
        return render_template(
            "airline_destination_edit.html",
            airline_id=airline_id,
            destination=destination,
        )

    require_csrf()
    dest_code = _sanitize(request.form.get("dest_code")).upper()
    dest_name = _sanitize(request.form.get("dest_name"))
    active = _parse_bool_checkbox(request.form.get("active"))
    now = _utc_now_iso()

    if not dest_name:
        flash("Destination name is required.")
        return redirect(
            url_for("airline_destination_edit", airline_id=airline_id, destination_id=destination_id)
        )

    with get_connection() as conn:
        cur = conn.cursor()
        if dest_code:
            cur.execute(
                """
                SELECT 1 FROM airline_destinations
                WHERE airline_id = ? AND dest_code = ? AND id != ?
                """,
                (airline_id, dest_code, destination_id),
            )
            if cur.fetchone():
                flash("Destination code must be unique for this airline.")
                return redirect(
                    url_for(
                        "airline_destination_edit",
                        airline_id=airline_id,
                        destination_id=destination_id,
                    )
                )

        cur.execute(
            """
            UPDATE airline_destinations
            SET dest_code = ?, dest_name = ?, active = ?, updated_at_utc = ?
            WHERE id = ? AND airline_id = ?
            """,
            (dest_code or None, dest_name, active, now, destination_id, airline_id),
        )
        conn.commit()

    flash("Destination updated.")
    return redirect(url_for("airline_destinations", airline_id=airline_id))


@app.post(
    "/airlines/<int:airline_id>/destinations/<int:destination_id>/delete",
    endpoint="airline_destination_delete",
)
@admin_required
def airline_destination_delete(airline_id: int, destination_id: int):
    require_csrf()
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "DELETE FROM airline_destinations WHERE id = ? AND airline_id = ?",
                (destination_id, airline_id),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            flash("Destination is used in sales and cannot be deleted.")
            return redirect(url_for("airline_destinations", airline_id=airline_id))
    flash("Destination deleted.")
    return redirect(url_for("airline_destinations", airline_id=airline_id))


if __name__ == "__main__":
    app.run(debug=True)
