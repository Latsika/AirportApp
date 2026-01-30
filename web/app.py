# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from functools import wraps
from secrets import token_urlsafe
from typing import Optional

import smtplib
from flask import (
    Flask,
    abort,
    flash,
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
    set_app_state,
)
from utils.security import hash_password, verify_password_and_upgrade  # noqa: E402

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


def _redirect_after_login(role: str | None):
    if role in {"Admin", "Deputy"}:
        return redirect(url_for("admin_hub"))
    return redirect(url_for("profile"))


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
            flash("Admin privileges required.")
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


@app.before_request
def enforce_session_timeout_and_single_user():
    session.setdefault("csrf_token", token_urlsafe(32))

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

    if a1 != (user["a1"] or "") or a2 != (user["a2"] or "") or a3 != (user["a3"] or ""):
        flash("❌ Answers do not match.")
        return redirect(url_for("forgot"))

    new_password = request.form.get("new_password") or ""
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


@app.get("/admin_page", endpoint="admin_page")
@admin_required
def admin_page():
    return render_template("admin_page.html")


@app.get("/sales", endpoint="sales")
@admin_required
def sales():
    return render_template("sales.html")


@app.get("/reports", endpoint="reports")
@admin_required
def reports():
    return render_template("reports.html")


@app.get("/variable_rewards", endpoint="variable_rewards")
@admin_required
def variable_rewards():
    return render_template("variable_rewards.html")


@app.get("/account_settings", endpoint="account_settings")
@admin_required
def account_settings():
    return render_template("account_settings.html")


@app.get("/notifications", endpoint="notifications")
@admin_required
def notifications():
    return render_template("notifications.html")


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

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT role FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            flash("User not found.")
            return redirect(url_for("users"))

        if row["role"] == "Admin" and _count_admins() <= 1:
            flash("You cannot delete the last Admin. Reassign Admin role first.")
            return redirect(url_for("reassign_admin"))

        cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()

    flash("User deleted.")
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

    return render_template("user_logs.html", user=user, logs=logs)


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


if __name__ == "__main__":
    app.run(debug=True)
