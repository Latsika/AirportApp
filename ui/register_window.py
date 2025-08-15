def register_user():
    print("\n=== User Registration ===")
    fullname = input("Full Name: ")
    nickname = input("Nickname: ")
    password = input("Password: ")
    q1 = input("Security Question 1 (e.g. Father's name): ")
    a1 = input("Answer: ")
    q2 = input("Security Question 2: ")
    a2 = input("Answer: ")
    q3 = input("Security Question 3: ")
    a3 = input("Answer: ")

    from database.db import get_connection
    from utils.security import hash_password

    with get_connection() as conn:
        c = conn.cursor()
        try:
            c.execute("""
                INSERT INTO users (fullname, nickname, password, role, q1, a1, q2, a2, q3, a3)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (fullname, nickname, hash_password(password), "User", q1, a1, q2, a2, q3, a3))
            conn.commit()
            print("\n✅ User registered successfully!")
        except Exception as e:
            print(f"\n❌ Registration failed: {e}")