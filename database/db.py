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


def _migrate_airport_service_fees_table(conn: sqlite3.Connection) -> None:
    cols = _get_columns(conn, "airport_service_fees")
    cur = conn.cursor()
    now = _utc_now_iso()

    if "fee_key" not in cols:
        cur.execute("ALTER TABLE airport_service_fees ADD COLUMN fee_key TEXT")
    if "fee_name" not in cols:
        cur.execute("ALTER TABLE airport_service_fees ADD COLUMN fee_name TEXT")
    if "amount" not in cols:
        cur.execute("ALTER TABLE airport_service_fees ADD COLUMN amount REAL NOT NULL DEFAULT 0")
    if "currency" not in cols:
        cur.execute("ALTER TABLE airport_service_fees ADD COLUMN currency TEXT NOT NULL DEFAULT 'EUR'")
    if "unit" not in cols:
        cur.execute("ALTER TABLE airport_service_fees ADD COLUMN unit TEXT")
    if "notes" not in cols:
        cur.execute("ALTER TABLE airport_service_fees ADD COLUMN notes TEXT")
    if "updated_at_utc" not in cols:
        cur.execute("ALTER TABLE airport_service_fees ADD COLUMN updated_at_utc TEXT")

    conn.commit()

    cur.execute(
        "UPDATE airport_service_fees SET updated_at_utc = ? WHERE updated_at_utc IS NULL OR updated_at_utc = ''",
        (now,),
    )
    conn.commit()

    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_airport_service_fee_key ON airport_service_fees(fee_key)")
    except sqlite3.OperationalError:
        pass
    conn.commit()


def _migrate_sales_table(conn: sqlite3.Connection) -> None:
    cols = _get_columns(conn, "sales")
    cur = conn.cursor()
    now = _utc_now_iso()

    if "sale_group_id" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN sale_group_id TEXT")
    if "airline_id" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airline_id INTEGER NOT NULL DEFAULT 0")
    if "fee_source" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN fee_source TEXT NOT NULL DEFAULT 'airline'")
    if "fee_id" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN fee_id INTEGER NOT NULL DEFAULT 0")
    if "fee_key" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN fee_key TEXT")
    if "fee_name" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN fee_name TEXT NOT NULL DEFAULT ''")
    if "amount" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN amount REAL NOT NULL DEFAULT 0")
    if "currency" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN currency TEXT NOT NULL DEFAULT 'EUR'")
    if "quantity" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN quantity INTEGER NOT NULL DEFAULT 1")
    if "total_amount" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN total_amount REAL NOT NULL DEFAULT 0")
    if "sold_at_utc" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN sold_at_utc TEXT NOT NULL DEFAULT ''")
    if "created_by" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN created_by INTEGER")
    if "payment_method" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN payment_method TEXT NOT NULL DEFAULT 'CASH'")
    if "cash_amount" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN cash_amount REAL NOT NULL DEFAULT 0")
    if "card_amount" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN card_amount REAL NOT NULL DEFAULT 0")
    if "airline_fee_id" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airline_fee_id INTEGER")
    if "airline_fee_key" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airline_fee_key TEXT")
    if "airline_fee_name" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airline_fee_name TEXT")
    if "airline_amount" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airline_amount REAL NOT NULL DEFAULT 0")
    if "airline_qty" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airline_qty INTEGER NOT NULL DEFAULT 1")
    if "airline_total" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airline_total REAL NOT NULL DEFAULT 0")
    if "airport_fee_id" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airport_fee_id INTEGER")
    if "airport_fee_key" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airport_fee_key TEXT")
    if "airport_fee_name" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airport_fee_name TEXT")
    if "airport_amount" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airport_amount REAL NOT NULL DEFAULT 0")
    if "airport_qty" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airport_qty INTEGER NOT NULL DEFAULT 1")
    if "airport_total" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN airport_total REAL NOT NULL DEFAULT 0")
    if "ticket_qty" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN ticket_qty INTEGER NOT NULL DEFAULT 0")
    if "ticket_amount" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN ticket_amount REAL NOT NULL DEFAULT 0")
    if "ticket_total" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN ticket_total REAL NOT NULL DEFAULT 0")
    if "grand_total" not in cols:
        cur.execute("ALTER TABLE sales ADD COLUMN grand_total REAL NOT NULL DEFAULT 0")

    conn.commit()

    cur.execute(
        "UPDATE sales SET sold_at_utc = ? WHERE sold_at_utc IS NULL OR sold_at_utc = ''",
        (now,),
    )
    conn.commit()

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_airline ON sales(airline_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sales_sold_at ON sales(sold_at_utc)")
    conn.commit()


def _migrate_sale_items_table(conn: sqlite3.Connection) -> None:
    cols = _get_columns(conn, "sale_items")
    cur = conn.cursor()
    now = _utc_now_iso()

    if "sale_id" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN sale_id INTEGER")
    if "fee_source" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN fee_source TEXT NOT NULL DEFAULT 'airline'")
    if "fee_id" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN fee_id INTEGER NOT NULL DEFAULT 0")
    if "fee_key" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN fee_key TEXT")
    if "fee_name" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN fee_name TEXT NOT NULL DEFAULT ''")
    if "amount" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN amount REAL NOT NULL DEFAULT 0")
    if "currency" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN currency TEXT NOT NULL DEFAULT 'EUR'")
    if "quantity" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN quantity INTEGER NOT NULL DEFAULT 1")
    if "total_amount" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN total_amount REAL NOT NULL DEFAULT 0")
    if "created_at_utc" not in cols:
        cur.execute("ALTER TABLE sale_items ADD COLUMN created_at_utc TEXT NOT NULL DEFAULT ''")

    conn.commit()

    cur.execute(
        "UPDATE sale_items SET created_at_utc = ? WHERE created_at_utc IS NULL OR created_at_utc = ''",
        (now,),
    )
    conn.commit()

    cur.execute("CREATE INDEX IF NOT EXISTS idx_sale_items_sale ON sale_items(sale_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sale_items_source ON sale_items(fee_source)")
    conn.commit()


def _backfill_sale_items(conn: sqlite3.Connection) -> None:
    """Backfill sale_items from legacy columns in sales for rows missing items."""
    cur = conn.cursor()
    cols = _get_columns(conn, "sales")
    if not _table_exists(conn, "sale_items"):
        return

    cur.execute(
        """
        SELECT s.*
        FROM sales s
        WHERE NOT EXISTS (
            SELECT 1 FROM sale_items si WHERE si.sale_id = s.id
        )
        """
    )
    rows = cur.fetchall()
    if not rows:
        return

    now = _utc_now_iso()

    def _insert_item(sale_id, source, fee_id, fee_key, fee_name, amount, currency, qty):
        total = (amount or 0) * (qty or 1)
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
                source,
                int(fee_id or 0),
                fee_key or "",
                fee_name or "",
                float(amount or 0),
                currency or "EUR",
                int(qty or 1),
                float(total),
                now,
            ),
        )

    for r in rows:
        sale_id = r["id"]
        r_dict = dict(r)
        # Newer legacy model: airline_fee_name / airport_fee_name / ticket
        if "airline_fee_name" in cols and (r_dict.get("airline_fee_name") or r_dict.get("airport_fee_name")):
            if r_dict.get("airline_fee_name"):
                _insert_item(
                    sale_id,
                    "airline",
                    r_dict.get("airline_fee_id") if "airline_fee_id" in cols else 0,
                    r_dict.get("airline_fee_key") if "airline_fee_key" in cols else "",
                    r_dict.get("airline_fee_name") or "",
                    r_dict.get("airline_amount") if "airline_amount" in cols else r_dict.get("amount"),
                    "EUR",
                    r_dict.get("airline_qty") if "airline_qty" in cols else 1,
                )
            if r_dict.get("airport_fee_name"):
                _insert_item(
                    sale_id,
                    "airport",
                    r_dict.get("airport_fee_id") if "airport_fee_id" in cols else 0,
                    r_dict.get("airport_fee_key") if "airport_fee_key" in cols else "",
                    r_dict.get("airport_fee_name") or "",
                    r_dict.get("airport_amount") if "airport_amount" in cols else r_dict.get("amount"),
                    "EUR",
                    r_dict.get("airport_qty") if "airport_qty" in cols else 1,
                )
            if "ticket_qty" in cols and (r_dict.get("ticket_qty") or 0) > 0:
                _insert_item(
                    sale_id,
                    "ticket",
                    0,
                    "TICKET",
                    "Ticket",
                    r_dict.get("ticket_amount") if "ticket_amount" in cols else 0,
                    "EUR",
                    r_dict.get("ticket_qty"),
                )
            continue

        # Older legacy model: fee_source/fee_name
        if "fee_name" in cols and r_dict.get("fee_name"):
            _insert_item(
                sale_id,
                r_dict.get("fee_source") or "airline",
                r_dict.get("fee_id") if "fee_id" in cols else 0,
                r_dict.get("fee_key") if "fee_key" in cols else "",
                r_dict.get("fee_name"),
                r_dict.get("amount") if "amount" in cols else 0,
                r_dict.get("currency") if "currency" in cols else "EUR",
                r_dict.get("quantity") if "quantity" in cols else 1,
            )

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

        # AIRPORT SERVICE FEES
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS airport_service_fees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fee_key TEXT NOT NULL,
                fee_name TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                unit TEXT,
                notes TEXT,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        conn.commit()
        _migrate_airport_service_fees_table(conn)

        # SALES
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_group_id TEXT,
                airline_id INTEGER NOT NULL,
                fee_source TEXT NOT NULL DEFAULT 'airline',
                fee_id INTEGER NOT NULL,
                fee_key TEXT,
                fee_name TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                quantity INTEGER NOT NULL DEFAULT 1,
                total_amount REAL NOT NULL DEFAULT 0,
                sold_at_utc TEXT NOT NULL,
                created_by INTEGER,
                payment_method TEXT NOT NULL DEFAULT 'CASH',
                cash_amount REAL NOT NULL DEFAULT 0,
                card_amount REAL NOT NULL DEFAULT 0,
                airline_fee_id INTEGER,
                airline_fee_key TEXT,
                airline_fee_name TEXT,
                airline_amount REAL NOT NULL DEFAULT 0,
                airline_qty INTEGER NOT NULL DEFAULT 1,
                airline_total REAL NOT NULL DEFAULT 0,
                airport_fee_id INTEGER,
                airport_fee_key TEXT,
                airport_fee_name TEXT,
                airport_amount REAL NOT NULL DEFAULT 0,
                airport_qty INTEGER NOT NULL DEFAULT 1,
                airport_total REAL NOT NULL DEFAULT 0,
                ticket_qty INTEGER NOT NULL DEFAULT 0,
                ticket_amount REAL NOT NULL DEFAULT 0,
                ticket_total REAL NOT NULL DEFAULT 0,
                grand_total REAL NOT NULL DEFAULT 0,
                FOREIGN KEY(airline_id) REFERENCES airlines(id) ON DELETE RESTRICT
            )
            """
        )
        conn.commit()
        _migrate_sales_table(conn)

        # SALE ITEMS
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sale_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_id INTEGER NOT NULL,
                fee_source TEXT NOT NULL DEFAULT 'airline',
                fee_id INTEGER NOT NULL,
                fee_key TEXT,
                fee_name TEXT NOT NULL,
                amount REAL NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                quantity INTEGER NOT NULL DEFAULT 1,
                total_amount REAL NOT NULL DEFAULT 0,
                created_at_utc TEXT NOT NULL,
                FOREIGN KEY(sale_id) REFERENCES sales(id) ON DELETE CASCADE
            )
            """
        )
        conn.commit()
        _migrate_sale_items_table(conn)
        _backfill_sale_items(conn)


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
