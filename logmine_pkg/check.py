import os

db_path = '/home/ubuntu/logmine/logmine_pkg/HDFS_2K.db'

print("Current Working Directory:", os.getcwd())

if os.path.exists(db_path):
    print(f"Database file {db_path} exists.")
else:
    print(f"Database file {db_path} does not exist or cannot be accessed.")
