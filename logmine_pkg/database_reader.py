import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

class DatabaseLogReader:
    def __init__(self, db_url, table_name):
        self.db_url = db_url
        self.table_name = table_name
        self._initialize_connection()

    def _initialize_connection(self):
        self.engine = db.create_engine(self.db_url)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        self.table = db.Table(self.table_name, self.metadata, autoload_with=self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def close(self):
        """Close the current connection and session to ensure no data carryover."""
        if self.session:
            self.session.close()
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()

    def read_logs(self, start_id, batch_size=None):
        query = db.select(self.table).where(self.table.c.id >= start_id)
        if batch_size:
            query = query.limit(batch_size)
        result_proxy = self.connection.execute(query)
        logs = result_proxy.fetchall()
        return logs

    def switch_database(self, new_db_url, new_table_name):
        """Switch to a new database and reinitialize the connection."""
        self.close()  # Close existing connections before switching
        self.db_url = new_db_url
        self.table_name = new_table_name
        self._initialize_connection()
