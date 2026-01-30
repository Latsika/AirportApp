import sqlite3
from pathlib import Path

# nastav presnú cestu k DB (uprav ak máš DB inde)
db_path = Path(r"C:\Users\A106035204\OneDrive - Deutsche Telekom AG\GITHUB REPO\AirportApp\AirportApp\airport_app.db")

conn = sqlite3.connect(db_path)
cur = conn.cursor()

cur.execute("""
UPDATE users
SET role = 'Admin'
WHERE nickname = 'Admin' OR fullname = 'Admin';
""")

conn.commit()

cur.execute("SELECT id, fullname, nickname, role FROM users WHERE nickname='Admin' OR fullname='Admin';")
print(cur.fetchall())

conn.close()
