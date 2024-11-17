import sqlite3

def create_test_db_from_log(log_file, db_name):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS log_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        log_message TEXT
    )
    ''')
    with open(log_file, 'r') as file:
        logs = file.readlines()
    for log in logs:
        cursor.execute('INSERT INTO log_table (log_message) VALUES (?)', (log,))
    connection.commit()
    connection.close()

# Example usage
log_file = '/home/ubuntu/logmine/sample/OpenSSH_2k.log'  # Ensure this is the correct path
db_name = 'OpenSSH_2k.db'
create_test_db_from_log(log_file, db_name)
