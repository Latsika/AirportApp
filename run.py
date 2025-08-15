# This version avoids GUI errors and ensures package accessibility in constrained environments

# run.py (root script)

import sys
import os

# Handle environments where __file__ might be undefined
def get_base_path():
    try:
        return os.path.abspath(os.path.dirname(__file__))
    except NameError:
        return os.getcwd()

base_path = get_base_path()
if base_path not in sys.path:
    sys.path.insert(0, base_path)

try:
    from database.db import init_db
    from models.user_model import create_admin_if_not_exists
except ModuleNotFoundError as e:
    print("ERROR:", e)
    print("Ensure you're running run.py from inside the airport_app folder as a module:")
    print("   python -m run")
else:
    def main():
        init_db()
        create_admin_if_not_exists()
        print("Airport App Initialized")
        print("Login window is currently not available due to missing GUI support.")

    if __name__ == "__main__":
        main()