# class LoginWindow:
#     def show(self):
#         print("Login GUI cannot be displayed. This is a CLI placeholder.")

# ui/login_window.py

from database.db import get_connection
from utils.security import verify_password

def login_user():
    print("\n=== Login ===")
    nickname = input("Nickname: ")
    password = input("Password: ")

    with get_connection() as conn:
        c = conn.cursor()
        c.execute("SELECT password FROM users WHERE nickname = ?", (nickname,))
        row = c.fetchone()
        if row and verify_password(password, row[0]):
            print("✅ Login successful!")
        else:
            print("❌ Invalid credentials.")
