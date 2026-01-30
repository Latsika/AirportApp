import os
import sqlite3
from datetime import datetime, timezone


# Absolútna cesta k DB súboru – spoľahlivé aj mimo projektu
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_NAME = os.path.abspath(os.path.join(BASE_DIR, "..", "airport_app.db"))


def get_db_path() -> str:
    """Return path to SQLite database.

    Allows override via env var AIRPORTAPP_DB_PATH (useful for tests).
    """
    return os.environ.get("AIRPORTAPP_DB_PATH", DEFAULT_DB_NAME)


def get_connection() -> sqlite3.Connection:
    """Create SQLite connection with Row factory for nicer templates."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    rows = cur.fetchall()
    return {row["name"] for row in rows}


def _migrate_users_table(conn: sqlite3.Connection) -> None:
    """Add missing columns to existing 'users' table (SQLite-safe migration)."""
    cols = _get_columns(conn, "users")
    cur = conn.cursor()

    if "must_change_password" not in cols:
        cur.execute(
            "ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0"
        )

    for col in ("q1", "a1", "q2", "a2", "q3", "a3"):
        if col not in cols:
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")

    conn.commit()


def _migrate_airlines_table(conn: sqlite3.Connection) -> None:
    """Add missing columns to existing 'airlines' table."""
    cols = _get_columns(conn, "airlines")
    cur = conn.cursor()
    now = _utc_now_iso()

    # Columns added over time (to avoid breaking existing DBs)
    if "name" not in cols:
        # should never happen, but keep defensive
        cur.execute("ALTER TABLE airlines ADD COLUMN name TEXT")

    if "code" not in cols:
        cur.execute("ALTER TABLE airlines ADD COLUMN code TEXT")

    if "country" not in cols:
        cur.execute("ALTER TABLE airlines ADD COLUMN country TEXT")

    if "active" not in cols:
        cur.execute("ALTER TABLE airlines ADD COLUMN active INTEGER NOT NULL DEFAULT 1")

    if "created_at_utc" not in cols:
        cur.execute("ALTER TABLE airlines ADD COLUMN created_at_utc TEXT")

    if "updated_at_utc" not in cols:
        cur.execute("ALTER TABLE airlines ADD COLUMN updated_at_utc TEXT")

    conn.commit()

    # Backfill timestamps if missing/empty
    if "created_at_utc" in _get_columns(conn, "airlines"):
        cur.execute(
            "UPDATE airlines SET created_at_utc = ? "
            "WHERE created_at_utc IS NULL OR created_at_utc = ''",
            (now,),
        )
    if "updated_at_utc" in _get_columns(conn, "airlines"):
        cur.execute(
            "UPDATE airlines SET updated_at_utc = ? "
            "WHERE updated_at_utc IS NULL OR updated_at_utc = ''",
            (now,),
        )
    conn.commit()

    # Create indexes (safe)
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_airlines_code ON airlines(code)")
    except sqlite3.OperationalError:
        # if duplicates exist, unique index creation would fail; keep app running
        pass

    cur.execute("CREATE INDEX IF NOT EXISTS idx_airlines_active ON airlines(active)")
    conn.commit()


def _migrate_airline_fees_table(conn: sqlite3.Connection) -> None:
    """Add missing columns to existing 'airline_fees' table."""
    cols = _get_columns(conn, "airline_fees")
    cur = conn.cursor()
    now = _utc_now_iso()

    # Fees schema is flexible; keep backward compatible
    if "airline_id" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN airline_id INTEGER")

    if "fee_key" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN fee_key TEXT")

    if "fee_name" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN fee_name TEXT")

    if "amount" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN amount REAL NOT NULL DEFAULT 0")

    if "currency" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN currency TEXT NOT NULL DEFAULT 'EUR'")

    if "unit" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN unit TEXT")

    if "notes" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN notes TEXT")

    if "updated_at_utc" not in cols:
        cur.execute("ALTER TABLE airline_fees ADD COLUMN updated_at_utc TEXT")

    conn.commit()

    # backfill updated_at_utc
    if "updated_at_utc" in _get_columns(conn, "airline_fees"):
        cur.execute(
            "UPDATE airline_fees SET updated_at_utc = ? "
            "WHERE updated_at_utc IS NULL OR updated_at_utc = ''",
            (now,),
        )
        conn.commit()

    # indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fees_airline ON airline_fees(airline_id)")
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_fee_airline_key ON airline_fees(airline_id, fee_key)")
    except sqlite3.OperationalError:
        pass

    conn.commit()


def init_db() -> None:
    """Initialize base schema and apply minimal migrations."""
    with get_connection() as conn:
        cur = conn.cursor()

        # Users
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
        _migrate_users_table(conn)

        # Auth logs
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                nickname TEXT,
                fullname TEXT,
                role TEXT,
                action TEXT NOT NULL,
                success INTEGER NOT NULL DEFAULT 1,
                ip TEXT,
                user_agent TEXT,
                details TEXT,
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_logs_created ON auth_logs(created_at_utc)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_logs_user ON auth_logs(user_id)")
        conn.commit()

        # Global app state (single active session)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.commit()

        # Airlines
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS airlines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                code TEXT,
                country TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                created_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        conn.commit()
        _migrate_airlines_table(conn)

        # Airline fees
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS airline_fees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                airline_id INTEGER NOT NULL,
                fee_key TEXT NOT NULL,
                fee_name TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                unit TEXT,
                notes TEXT,
                updated_at_utc TEXT NOT NULL,
                FOREIGN KEY(airline_id) REFERENCES airlines(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()
        _migrate_airline_fees_table(conn)


def log_auth_event(
    *,
    user_id: int | None,
    nickname: str | None,
    fullname: str | None,
    role: str | None,
    action: str,
    success: bool = True,
    ip: str | None = None,
    user_agent: str | None = None,
    details: str | None = None,
) -> None:
    """Write auth audit log."""
    created_at_utc = _utc_now_iso()

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO auth_logs (
                user_id, nickname, fullname, role, action, success,
                ip, user_agent, details, created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                nickname,
                fullname,
                role,
                action,
                1 if success else 0,
                ip,
                user_agent,
                details,
                created_at_utc,
            ),
        )
        conn.commit()


# -----------------------------------------------------------------------------
# Single active user helpers
# -----------------------------------------------------------------------------
def set_app_state(key: str, value: str) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO app_state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def get_app_state(key: str) -> str | None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM app_state WHERE key = ?", (key,))
        row = cur.fetchone()
    return row["value"] if row else None


def delete_app_state(key: str) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM app_state WHERE key = ?", (key,))
        conn.commit()


def ensure_default_admin(hash_password_func) -> None:
    """Create default Admin with password '12345' on first run.

    Force password change on first login.
    """
    from utils.security import hash_password as _hp  # local import to avoid cycles

    hasher = hash_password_func or _hp

    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM users WHERE nickname = 'Admin'")
        if cur.fetchone():
            return

        cols = _get_columns(conn, "users")
        if "must_change_password" in cols:
            cur.execute(
                """
                INSERT INTO users (fullname, nickname, password, role, must_change_password)
                VALUES (?, ?, ?, ?, 1)
                """,
                ("Admin", "Admin", hasher("12345"), "Admin"),
            )
        else:
            cur.execute(
                """
                INSERT INTO users (fullname, nickname, password, role)
                VALUES (?, ?, ?, ?)
                """,
                ("Admin", "Admin", hasher("12345"), "Admin"),
            )

        conn.commit()
