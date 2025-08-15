from flask import Flask, render_template, request, redirect, flash, url_for, session
import os
from datetime import datetime
from database.db import init_db, get_connection
from utils.security import verify_password, hash_password

# Inicializuj databázu
init_db()

# Vytvor default admina
with get_connection() as conn:
    c = conn.cursor()
    c.execute("SELECT 1 FROM users WHERE nickname = 'Admin'")
    if not c.fetchone():
        c.execute("INSERT INTO users (nickname, fullname, password, role) VALUES (?, ?, ?, ?)",
                  ("Admin", "Admin", hash_password("12345"), "Admin"))
        conn.commit()

app = Flask(__name__)
app.secret_key = 'secret123'

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        nickname = request.form["nickname"]
        fullname = request.form["fullname"]
        password = request.form["password"]
        with get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM users WHERE nickname = ?", (nickname,))
            if c.fetchone():
                flash("❌ Nickname already exists")
                return redirect(url_for("register"))
            try:
                c.execute("INSERT INTO users (nickname, fullname, password, role) VALUES (?, ?, ?, ?)",
                          (nickname, fullname, hash_password(password), "User"))
                conn.commit()
                flash("✅ Registration successful. You can now log in.")
                return redirect(url_for("index"))
            except Exception as e:
                flash("❌ Registration failed: " + str(e))
                return redirect(url_for("register"))
    return render_template("register.html")

@app.route("/login", methods=["POST"])
def login():
    nickname = request.form["nickname"]
    password = request.form["password"]
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT password, role FROM users WHERE nickname = ?", (nickname,))
        row = c.fetchone()
        if row and verify_password(password, row[0]):
            session['nickname'] = nickname
            session['role'] = row[1]
            flash("✅ Login successful")
            if row[1] == "Admin":
                return redirect(url_for("admin_page"))
            return redirect(url_for("index"))
        flash("❌ Invalid credentials")
        return redirect(url_for("index"))

@app.route("/admin")
def admin_page():
    if session.get("role") != "Admin":
        flash("❌ Unauthorized access")
        return redirect(url_for("index"))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM users")
        all_users = c.fetchall()
    return render_template("admin.html", users=all_users)

@app.route("/users")
def users():
    if session.get("role") != "Admin":
        flash("❌ Unauthorized access")
        return redirect(url_for("index"))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT id, fullname, nickname, role FROM users")
        user_list = c.fetchall()
    return render_template("users.html", users=user_list)

@app.route("/edit_user/<int:user_id>", methods=["GET", "POST"])
def edit_user(user_id):
    if session.get("role") != "Admin":
        flash("❌ Unauthorized access")
        return redirect(url_for("index"))
    with get_connection() as conn:
        c = conn.cursor()
        if request.method == "POST":
            fullname = request.form["fullname"]
            nickname = request.form["nickname"]
            role = request.form["role"]
            if role not in ["User", "Admin"]:
                flash("❌ Invalid role value")
                return redirect(url_for("edit_user", user_id=user_id))
            c.execute("UPDATE users SET fullname = ?, nickname = ?, role = ? WHERE id = ?",
                      (fullname, nickname, role, user_id))
            conn.commit()
            flash("✅ User updated successfully")
            return redirect(url_for("users"))
        c.execute("SELECT id, fullname, nickname, role FROM users WHERE id = ?", (user_id,))
        user = c.fetchone()
    return render_template("edit_user.html", user=user)

@app.route("/reassign_admin", methods=["GET", "POST"])
def reassign_admin():
    if request.method == "POST":
        new_admin_id = request.form.get("admin_id")
        with get_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE users SET role = 'Admin' WHERE id = ?", (new_admin_id,))
            conn.commit()
        flash("✅ Admin role reassigned successfully")
        return redirect(url_for("users"))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT id, nickname FROM users WHERE role != 'Admin'")
        candidates = c.fetchall()
    return render_template("reassign_admin.html", users=candidates)

@app.route("/delete_user/<int:user_id>", methods=["GET", "POST"])
def delete_user(user_id):
    if session.get("role") != "Admin":
        flash("❌ Unauthorized access")
        return redirect(url_for("index"))
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT role FROM users WHERE id = ?", (user_id,))
        user = c.fetchone()
        if not user:
            flash("❌ User not found")
            return redirect(url_for("users"))

        is_admin = user[0] == "Admin"
        if is_admin:
            c.execute("SELECT COUNT(*) FROM users WHERE role = 'Admin'")
            admin_count = c.fetchone()[0]
            if admin_count == 1:
                return redirect(url_for("reassign_admin"))

        c.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
    flash("✅ User deleted")
    return redirect(url_for("users"))

if __name__ == "__main__":
    app.run(debug=True)
