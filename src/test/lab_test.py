import unittest
import sqlite3
import os
import csv  # Added this import
from src.main.main import (return_cursor, create_tables, load_and_clean_users, 
                          load_and_clean_call_logs, write_ordered_calls, write_user_analytics)

class ProjectTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        create_tables(self.conn)
        self.cursor = return_cursor(self.conn)

        # Go up two levels from src/test/ to reach project root
        self.base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.resources_dir = os.path.join(self.base_dir, 'resources')
        self.users_file = os.path.join(self.resources_dir, 'users.csv')
        self.call_logs_file = os.path.join(self.resources_dir, 'callLogs.csv')
        self.user_analytics_file = os.path.join(self.resources_dir, 'userAnalytics.csv')
        self.ordered_call_logs_file = os.path.join(self.resources_dir, 'orderedCallLogs.csv')

    def tearDown(self):
        self.conn.close()
        for file in [self.user_analytics_file, self.ordered_call_logs_file]:
            if os.path.exists(file):
                os.remove(file)

    def test_users_table_has_clean_data(self):
        load_and_clean_users(self.users_file, self.conn)
        self.cursor.execute("SELECT * FROM users")
        users = self.cursor.fetchall()
        self.assertTrue(len(users) > 0, "Users table should have data")
        for user in users:
            self.assertTrue(user[1].strip(), "firstName should not be empty")
            self.assertTrue(user[2].strip(), "lastName should not be empty")

    def test_calllogs_table_has_clean_data(self):
        load_and_clean_call_logs(self.call_logs_file, self.conn)
        self.cursor.execute("SELECT * FROM callLogs")
        call_logs = self.cursor.fetchall()
        self.assertTrue(len(call_logs) > 0, "CallLogs table should have data")
        for log in call_logs:
            self.assertTrue(log[1].strip(), "phoneNumber should not be empty")
            self.assertIsInstance(log[2], int, "startTimeEpoch should be an integer")
            self.assertIsInstance(log[3], int, "endTimeEpoch should be an integer")
            #self.assertTrue(log[4].strip(), "call direction should not be empty")
            #self.assertIn(log[4], ["inbound", "outbound"], "call direction should be 'inbound' or 'outbound'")
            self.assertTrue(log[5] > 0, "userId should be positive")

    def test_call_logs_are_ordered(self):
        load_and_clean_call_logs(self.call_logs_file, self.conn)
        write_ordered_calls(self.ordered_call_logs_file, self.conn)
        
        with open(self.ordered_call_logs_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            rows = list(reader)
            self.assertTrue(len(rows) > 0, "Ordered call logs should have data")
            prev_user_id, prev_start_time = None, None
            for row in rows:
                user_id = int(row[5])
                start_time = int(row[2])
                if prev_user_id is not None:
                    if user_id == prev_user_id:
                        self.assertTrue(start_time >= prev_start_time, "Start times should be ordered within userId")
                    else:
                        self.assertTrue(user_id > prev_user_id, "UserIds should be in ascending order")
                prev_user_id, prev_start_time = user_id, start_time

    def test_user_analytics_are_correct(self):
        load_and_clean_call_logs(self.call_logs_file, self.conn)
        write_user_analytics(self.user_analytics_file, self.conn)
        
        with open(self.user_analytics_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip header
            analytics = list(reader)
            self.assertTrue(len(analytics) > 0, "User analytics should have data")
            for row in analytics:
                user_id = int(row[0])
                avg_duration = float(row[1])
                num_calls = int(row[2])
                self.assertTrue(avg_duration >= 0, "Average duration should be non-negative")
                self.assertTrue(num_calls > 0, "Number of calls should be positive")

if __name__ == 'main':
   unittest.main()