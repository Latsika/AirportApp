from database.db import get_connection


def create_admin_if_not_exists():
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT * FROM users WHERE role = 'Admin'")
        if not c.fetchone():
            c.execute("""
                INSERT INTO users (fullname, nickname, password, role)
                VALUES (?, ?, ?, ?)
            """, ("Admin User", "admin", "12345", "Admin"))
            conn.commit()