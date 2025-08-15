import os
import re
from datetime import timedelta
from functools import wraps
from secrets import token_urlsafe

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    flash,
    url_for,
    session,
    abort,
)

from database.db import init_db, get_connection, ensure_default_admin
from utils.security import verify_password, hash_password


# -----------------------------------------------------------------------------
# Flask app & security configuration
# -----------------------------------------------------------------------------
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-please-change")  # v prod prostredí určite zmeniť
app.config["SESSION_PERMANENT"] = True
app.permanent_session_lifetime = timedelta(minutes=10)

# Init DB & default Admin
init_db()
ensure_default_admin(hash_password)


# -----------------------------------------------------------------------------
# Helpers: auth guards & CSRF
# -----------------------------------------------------------------------------
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


def require_csrf():
    """Minimal CSRF – token do session + do POST formulárov."""
    token = request.form.get("csrf_token")
    if not token or token != session.get("csrf_token"):
        abort(400)


@app.before_request
def enforce_session_timeout():
    # rolling session – každá request obnoví čas
    if session.get("logged_in"):
        session.permanent = True
        # Vytvor CSRF token, ak chýba
        if not session.get("csrf_token"):
            session["csrf_token"] = token_urlsafe(32)


# -----------------------------------------------------------------------------
# Password policy
# -----------------------------------------------------------------------------
PASSWORD_RE = re.compile(r"^(?=.*[A-Za-z])(?=.*\d)(?=.*[^A-Za-z0-9]).{8,}$")


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    # Login page
    return render_template("index.html")


@app.route("/login", methods=["POST"])
def login():
    require_csrf()
    nickname_or_fullname = request.form.get("nickname", "").strip()
    password = request.form.get("password", "")

    with get_connection() as conn:
        cur = conn.cursor()
        # Podľa zadania možno login nick+heslo alebo fullname+heslo
        cur.execute(
            "SELECT id, fullname, nickname, password, role, must_change_password FROM users "
            "WHERE nickname = ? OR fullname = ?",
            (nickname_or_fullname, nickname_or_fullname),
        )
        row = cur.fetchone()

    if not row or not verify_password(password, row["password"]):
        flash("❌ Invalid credentials")
        return redirect(url_for("index"))

    # Úspešný login
    session["logged_in"] = True
    session["user_id"] = row["id"]
    session["nickname"] = row["nickname"]
    session["fullname"] = row["fullname"]
    session["role"] = row["role"]
    session.setdefault("csrf_token", token_urlsafe(32))

    # Vynútenie zmeny hesla
    if row["must_change_password"]:
        return redirect(url_for("change_password"))

    # Admin dostane admin stránku, user budúci Main Page (zatiaľ admin stránka ako placeholder)
    if row["role"] == "Admin":
        return redirect(url_for("admin"))
    return redirect(url_for("admin"))  # dočasne – kým nepridáme hlavnú stránku


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.")
    return redirect(url_for("index"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "GET":
        return render_template("register.html")

    # POST
    require_csrf()
    fullname = (request.form.get("fullname") or "").strip()
    nickname = (request.form.get("nickname") or "").strip()
    password = request.form.get("password") or ""
    q1 = (request.form.get("q1") or "").strip()
    a1 = (request.form.get("a1") or "").strip()
    q2 = (request.form.get("q2") or "").strip()
    a2 = (request.form.get("a2") or "").strip()
    q3 = (request.form.get("q3") or "").strip()
    a3 = (request.form.get("a3") or "").strip()

    if not PASSWORD_RE.match(password):
        flash("Password must be 8+ chars incl. letters, digits and special char.")
        return redirect(url_for("register"))

    if not (fullname and nickname and q1 and a1 and q2 and a2 and q3 and a3):
        flash("Please fill all fields including 3 security questions.")
        return redirect(url_for("register"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE nickname = ?", (nickname,))
        if cur.fetchone():
            flash("❌ Nickname already exists")
            return redirect(url_for("register"))

        cur.execute(
            """
            INSERT INTO users (fullname, nickname, password, role, q1, a1, q2, a2, q3, a3)
            VALUES (?, ?, ?, 'User', ?, ?, ?, ?, ?, ?)
            """,
            (fullname, nickname, hash_password(password), q1, a1, q2, a2, q3, a3),
        )
        conn.commit()

    flash("✅ Registration successful. You can log in now.")
    return redirect(url_for("index"))


@app.route("/change_password", methods=["GET", "POST"])
@login_required
def change_password():
    # stránka na vynútenú zmenu hesla pre usera s must_change_password=1
    if request.method == "GET":
        return render_template("change_password.html")

    require_csrf()
    new_password = request.form.get("new_password") or ""
    if not PASSWORD_RE.match(new_password):
        flash("Password must be 8+ chars incl. letters, digits and special char.")
        return redirect(url_for("change_password"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET password = ?, must_change_password = 0 WHERE id = ?",
            (hash_password(new_password), session["user_id"]),
        )
        conn.commit()

    flash("✅ Password updated.")
    return redirect(url_for("admin" if session.get("role") == "Admin" else "index"))


@app.route("/forgot", methods=["GET", "POST"])
def forgot():
    # Overenie 3 odpovedí + reset hesla
    if request.method == "GET":
        return render_template("forgot.html")

    require_csrf()
    nickname = (request.form.get("nickname") or "").strip()
    a1 = (request.form.get("a1") or "").strip()
    a2 = (request.form.get("a2") or "").strip()
    a3 = (request.form.get("a3") or "").strip()
    new_password = request.form.get("new_password") or ""

    if not PASSWORD_RE.match(new_password):
        flash("Password must be 8+ chars incl. letters, digits and special char.")
        return redirect(url_for("forgot"))

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, a1, a2, a3 FROM users WHERE nickname = ?",
            (nickname,),
        )
        row = cur.fetchone()
        if not row:
            flash("❌ Unknown user.")
            return redirect(url_for("forgot"))

        if (a1 != (row["a1"] or "")) or (a2 != (row["a2"] or "")) or (a3 != (row["a3"] or "")):
            flash("❌ Answers do not match.")
            return redirect(url_for("forgot"))

        cur.execute(
            "UPDATE users SET password = ?, must_change_password = 0 WHERE id = ?",
            (hash_password(new_password), row["id"]),
        )
        conn.commit()

    flash("✅ Password reset. You can log in.")
    return redirect(url_for("index"))


# ----------------------- Admin & User management ------------------------------
@app.route("/admin")
@admin_required
def admin():
    return render_template("admin.html")


@app.route("/users")
@admin_required
def users():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, fullname, nickname, role FROM users ORDER BY id ASC")
        rows = cur.fetchall()
    return render_template("users.html", users=rows)


@app.route("/edit_user/<int:user_id>", methods=["GET", "POST"])
@admin_required
def edit_user(user_id: int):
    with get_connection() as conn:
        cur = conn.cursor()
        if request.method == "GET":
            cur.execute("SELECT id, fullname, nickname, role FROM users WHERE id = ?", (user_id,))
            row = cur.fetchone()
            if not row:
                flash("User not found.")
                return redirect(url_for("users"))
            return render_template("edit_users.html", user=row)

        # POST
        require_csrf()
        fullname = (request.form.get("fullname") or "").strip()
        nickname = (request.form.get("nickname") or "").strip()
        role = request.form.get("role") or "User"
        if role not in ("User", "Admin"):
            flash("Invalid role.")
            return redirect(url_for("edit_user", user_id=user_id))

        cur.execute(
            "UPDATE users SET fullname = ?, nickname = ?, role = ? WHERE id = ?",
            (fullname, nickname, role, user_id),
        )
        conn.commit()
        flash("✅ User updated")
        return redirect(url_for("users"))


@app.route("/delete_user/<int:user_id>")
@admin_required
def delete_user(user_id: int):
    with get_connection() as conn:
        cur = conn.cursor()

        # zakáž zmazanie posledného Admina
        cur.execute("SELECT role, nickname FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            flash("User not found.")
            return redirect(url_for("users"))

        if row["role"] == "Admin":
            cur.execute("SELECT COUNT(*) FROM users WHERE role = 'Admin'")
            admin_count = cur.fetchone()[0]
            if admin_count <= 1:
                flash("Cannot delete the last Admin. Reassign first.")
                return redirect(url_for("users"))

        cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()

    flash("✅ User deleted")
    return redirect(url_for("users"))


@app.route("/reassign_admin", methods=["GET", "POST"])
@admin_required
def reassign_admin():
    with get_connection() as conn:
        cur = conn.cursor()
        if request.method == "GET":
            cur.execute("SELECT id, fullname FROM users WHERE role = 'User' ORDER BY fullname")
            rows = cur.fetchall()
            return render_template("reassign_admin.html", users=rows)

        require_csrf()
        new_admin_id = int(request.form.get("admin_id"))

        # aktuálneho admina znížime na User, vybraného povýšime
        cur.execute("UPDATE users SET role = 'User' WHERE role = 'Admin'")
        cur.execute("UPDATE users SET role = 'Admin' WHERE id = ?", (new_admin_id,))
        conn.commit()

    flash("✅ Admin role reassigned.")
    return redirect(url_for("users"))


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True)
