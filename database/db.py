import sqlite3

DB_NAME = "airport_app.db"

def get_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fullname TEXT NOT NULL,
                nickname TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('User', 'Admin')),
                q1 TEXT, a1 TEXT,
                q2 TEXT, a2 TEXT,
                q3 TEXT, a3 TEXT
            )
        """)
        conn.commit()