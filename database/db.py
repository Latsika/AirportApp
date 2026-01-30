import os
import sqlite3
from datetime import datetime, timezone


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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _copy_table_data(conn: sqlite3.Connection, src: str, dst: str) -> None:
    """Copy common columns from src table to dst table."""
    src_cols = _get_columns(conn, src)
    dst_cols = _get_columns(conn, dst)
    common = sorted(list(src_cols.intersection(dst_cols)))
    if not common:
        return
    cols_csv = ", ".join(common)
    cur = conn.cursor()
    cur.execute(f"INSERT INTO {dst} ({cols_csv}) SELECT {cols_csv} FROM {src}")


def _auth_logs_references_users_old(conn: sqlite3.Connection) -> bool:
    """
    Robust detection if auth_logs has FK pointing to users_old.
    Works even when users_old table is missing.
    """
    if not _table_exists(conn, "auth_logs"):
        return False

    cur = conn.cursor()
    cur.execute("PRAGMA foreign_key_list(auth_logs)")
    fks = cur.fetchall()
    for fk in fks:
        # fk['table'] is the referenced table name
        if (fk["table"] or "").lower() == "users_old":
            return True

    # As a fallback, also check raw schema text for any mention (quoted/unquoted)
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='auth_logs'")
    row = cur.fetchone()
    sql = (row["sql"] or "") if row else ""
    return "users_old" in sql.lower()


def _rebuild_auth_logs(conn: sqlite3.Connection) -> None:
    """
    Rebuild auth_logs to reference users(id) instead of users_old.
    Preserves data.
    """
    cur = conn.cursor()

    conn.execute("PRAGMA foreign_keys = OFF;")
    conn.commit()

    # Rename old auth_logs
    cur.execute("ALTER TABLE auth_logs RENAME TO auth_logs_old;")

    # Create correct auth_logs
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

    # Copy data back
    _copy_table_data(conn, "auth_logs_old", "auth_logs")

    # Drop old
    cur.execute("DROP TABLE auth_logs_old;")

    # Recreate indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_logs_created ON auth_logs(created_at_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_auth_logs_user ON auth_logs(user_id)")

    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.commit()


def _rebuild_users_table_if_needed(conn: sqlite3.Connection) -> None:
    """
    SQLite does not allow easy ALTER of CHECK constraints.
    If users table CHECK(role IN ('User','Admin')) doesn't include Deputy, rebuild.

    WARNING:
    Renaming 'users' -> 'users_old' rewrites FKs in other tables to users_old.
    So we must repair auth_logs after rebuild (before users_old is dropped).
    """
    if not _table_exists(conn, "users"):
        return

    cur = conn.cursor()
    cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='users'")
    row = cur.fetchone()
    sql = (row["sql"] or "") if row else ""
    needs_rebuild = ("check" in sql.lower() and "role" in sql.lower() and "deputy" not in sql.lower())

    if not needs_rebuild:
        return

    now = _utc_now_iso()

    conn.execute("PRAGMA foreign_keys = OFF;")
    conn.commit()

    cur.execute("ALTER TABLE users RENAME TO users_old;")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fullname TEXT NOT NULL,
            nickname TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('User', 'Admin', 'Deputy')),
            must_change_password INTEGER NOT NULL DEFAULT 0,
            approved INTEGER NOT NULL DEFAULT 1,
            approved_by INTEGER,
            approved_at_utc TEXT,
            created_at_utc TEXT,
            q1 TEXT, a1 TEXT,
            q2 TEXT, a2 TEXT,
            q3 TEXT, a3 TEXT
        )
        """
    )

    _copy_table_data(conn, "users_old", "users")

    # Backfill new cols
    cur.execute(
        "UPDATE users SET created_at_utc = ? WHERE created_at_utc IS NULL OR created_at_utc = ''",
        (now,),
    )
    cur.execute("UPDATE users SET approved = 1 WHERE approved IS NULL;")
    conn.commit()

    # Important: repair auth_logs if it got rewritten to reference users_old
    if _auth_logs_references_users_old(conn):
        _rebuild_auth_logs(conn)

    # Now safe to drop users_old
    cur.execute("DROP TABLE users_old;")
    conn.commit()

    conn.execute("PRAGMA foreign_keys = ON;")
    conn.commit()


def _migrate_users_table(conn: sqlite3.Connection) -> None:
    """Add missing columns to existing 'users' table."""
    _rebuild_users_table_if_needed(conn)

    cols = _get_columns(conn, "users")
    cur = conn.cursor()
    now = _utc_now_iso()

    if "must_change_password" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0")

    added_approved = False
    if "approved" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN approved INTEGER NOT NULL DEFAULT 1")
        added_approved = True
    if "approved_by" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN approved_by INTEGER")
    if "approved_at_utc" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN approved_at_utc TEXT")
    if "created_at_utc" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN created_at_utc TEXT")

    for col in ("q1", "a1", "q2", "a2", "q3", "a3"):
        if col not in cols:
            cur.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT")

    conn.commit()

    # Backfill created_at_utc
    cur.execute(
        "UPDATE users SET created_at_utc = ? WHERE created_at_utc IS NULL OR created_at_utc = ''",
        (now,),
    )

    # If approved was newly added, approve existing accounts to avoid lockout
    if added_approved:
        cur.execute("UPDATE users SET approved = 1")
        cur.execute(
            "UPDATE users SET approved_at_utc = ? WHERE approved_at_utc IS NULL OR approved_at_utc = ''",
            (now,),
        )

    conn.commit()


def _migrate_auth_logs_table(conn: sqlite3.Connection) -> None:
    """Ensure auth_logs exists and references users(id), not users_old."""
    cur = conn.cursor()
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

    # If broken, rebuild it
    if _auth_logs_references_users_old(conn):
        _rebuild_auth_logs(conn)


def _migrate_airlines_table(conn: sqlite3.Connection) -> None:
    cols = _get_columns(conn, "airlines")
    cur = conn.cursor()
    now = _utc_now_iso()

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

    cur.execute(
        "UPDATE airlines SET created_at_utc = ? WHERE created_at_utc IS NULL OR created_at_utc = ''",
        (now,),
    )
    cur.execute(
        "UPDATE airlines SET updated_at_utc = ? WHERE updated_at_utc IS NULL OR updated_at_utc = ''",
        (now,),
    )
    conn.commit()

    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_airlines_code ON airlines(code)")
    except sqlite3.OperationalError:
        # duplicates might exist, don't crash app
        pass

    cur.execute("CREATE INDEX IF NOT EXISTS idx_airlines_active ON airlines(active)")
    conn.commit()


def _migrate_airline_fees_table(conn: sqlite3.Connection) -> None:
    cols = _get_columns(conn, "airline_fees")
    cur = conn.cursor()
    now = _utc_now_iso()

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

    cur.execute(
        "UPDATE airline_fees SET updated_at_utc = ? WHERE updated_at_utc IS NULL OR updated_at_utc = ''",
        (now,),
    )
    conn.commit()

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

        # USERS
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fullname TEXT NOT NULL,
                nickname TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('User', 'Admin', 'Deputy')),
                must_change_password INTEGER NOT NULL DEFAULT 0,
                approved INTEGER NOT NULL DEFAULT 1,
                approved_by INTEGER,
                approved_at_utc TEXT,
                created_at_utc TEXT,
                q1 TEXT, a1 TEXT,
                q2 TEXT, a2 TEXT,
                q3 TEXT, a3 TEXT
            )
            """
        )
        conn.commit()

        _migrate_users_table(conn)

        # AUTH LOGS (must be repaired if FK references users_old)
        _migrate_auth_logs_table(conn)

        # APP STATE
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.commit()

        # AIRLINES
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

        # FEES
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
    """Create default Admin with password '12345' on first run. Force password change."""
    from utils.security import hash_password as _hp  # local import to avoid cycles

    hasher = hash_password_func or _hp
    now = _utc_now_iso()

    with get_connection() as conn:
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM users WHERE nickname = 'Admin'")
        if cur.fetchone():
            return

        cols = _get_columns(conn, "users")

        columns = ["fullname", "nickname", "password", "role"]
        params = ["Admin", "Admin", hasher("12345"), "Admin"]

        if "must_change_password" in cols:
            columns.append("must_change_password")
            params.append(1)
        if "approved" in cols:
            columns.append("approved")
            params.append(1)
        if "approved_at_utc" in cols:
            columns.append("approved_at_utc")
            params.append(now)
        if "created_at_utc" in cols:
            columns.append("created_at_utc")
            params.append(now)

        cur.execute(
            f"INSERT INTO users ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(params))})",
            tuple(params),
        )
        conn.commit()
