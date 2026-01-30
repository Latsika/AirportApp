# tests/test_db.py

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from database.db import init_db, get_connection, get_db_path

class TestDatabaseInitialization(unittest.TestCase):

    def setUp(self):
        if os.path.exists(get_db_path()):
            os.remove(get_db_path())

    def test_users_table_created(self):
        init_db()
        with get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
            result = c.fetchone()
            self.assertIsNotNone(result)
            self.assertEqual(result[0], 'users')

    def tearDown(self):
        if os.path.exists(get_db_path()):
            os.remove(get_db_path())

if __name__ == '__main__':
    unittest.main()