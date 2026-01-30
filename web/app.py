# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from secrets import token_urlsafe
from typing import Optional

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


def _client_ip() -> Optional[str]:
    return request.headers.get("X-Forwarded-For", request.remote_addr)


def _user_agent() -> Optional[str]:
    return request.headers.get("User-Agent")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _redirect_after_login(role: str | None):
    if role == "Admin":
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


def require_csrf() -> None:
    token = request.form.get("csrf_token")
    if not token or token != session.get("csrf_token"):
        abort(400)


def _sanitize(value: str) -> str:
    return (value or "").strip()


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
            "SELECT id, fullname, nickname, password, role, must_change_password "
            "FROM users WHERE nickname = ? OR fullname = ?",
            (identifier, identifier),
        )
        row = cur.fetchone()

    if not row:
        flash("❌ Invalid credentials")
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

    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO users (fullname, nickname, password, role, q1, a1, q2, a2, q3, a3)
                VALUES (?, ?, ?, 'User', ?, ?, ?, ?, ?, ?)
                """,
                (fullname, nickname, hash_password(password), q1, a1, q2, a2, q3, a3),
            )
            conn.commit()
    except Exception:
        flash("❌ Nickname already exists.")
        return redirect(url_for("register"))

    flash("✅ User created. You can login now.")
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


@app.get("/admin_hub", endpoint="admin_hub")
@admin_required
def admin_hub():
    return render_template("admin_hub.html")


@app.get("/admin_page", endpoint="admin_page")
@admin_required
def admin_page():
    return render_template("admin_page.html")


@app.get("/admin", endpoint="admin")
@admin_required
def admin():
    return redirect(url_for("admin_page"))


# ✅ NEW: separate pages instead of panels
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


# Users management (existing)
@app.get("/users", endpoint="users")
@admin_required
def users():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, fullname, nickname, role FROM users ORDER BY id ASC")
        all_users = cur.fetchall()
    return render_template("users.html", users=all_users)


# --- Airlines & Fees routes: nechávam podľa tvojej aktuálnej implementácie ---
# (tu predpokladám, že už ich máš v app.py; ak nie, pošli mi tvoj app.py a zladíme)

if __name__ == "__main__":
    app.run(debug=True)
