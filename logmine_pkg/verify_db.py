import sqlite3

def verify_db_content(db_name):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table";')
    tables = cursor.fetchall()
    print("Tables in database:", tables)  # Print the table names instead
    connection.close()

# Example usage
db_name = 'test_logs.db'
verify_db_content(db_name)
