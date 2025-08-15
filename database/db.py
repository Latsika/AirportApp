import os
import sqlite3
from typing import Generator, Tuple

# Absolútna cesta k DB súboru – spoľahlivé aj mimo projektu
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.abspath(os.path.join(BASE_DIR, "..", "airport_app.db"))


def get_connection() -> sqlite3.Connection:
    """Create SQLite connection with Row factory for nicer templates."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Initialize base schema required for Commit 1 + 2."""
    with get_connection() as conn:
        cur = conn.cursor()
        # Users, vrátane flagu na vynútenie zmeny hesla
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fullname TEXT NOT NULL,
                nickname TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('User', 'Admin')),
                must_change_password INTEGER NOT NULL DEFAULT 0,
                q1 TEXT, a1 TEXT,
                q2 TEXT, a2 TEXT,
                q3 TEXT, a3 TEXT
            )
            """
        )
        conn.commit()


def ensure_default_admin(hash_password_func) -> None:
    """
    Create default Admin with password '12345' on first run.
    Force password change on first login.
    """
    from utils.security import hash_password as _hp  # local import to avoid cycles
    hasher = hash_password_func or _hp

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE nickname = 'Admin'")
        if not cur.fetchone():
            cur.execute(
                """
                INSERT INTO users (fullname, nickname, password, role, must_change_password)
                VALUES (?, ?, ?, ?, 1)
                """,
                ("Admin", "Admin", hasher("12345"), "Admin"),
            )
            conn.commit()
