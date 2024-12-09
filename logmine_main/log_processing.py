import os
import re
import sqlalchemy as db
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from typing import List, Tuple, Any
from .utils import Variable


# Database operations
class DatabaseLogReader:
    """
    Manages database connections and log reading operations.
    Supports flexible database interactions and log retrieval.
    """

    def __init__(self, db_url: str, table_name: str):
        self.db_url = db_url
        self.table_name = table_name
        self._initialize_connection()
        self.last_processed_id = 0

    def _initialize_connection(self):
        """
        Establish database connection and prepare session.
        Uses SQLAlchemy for database abstraction.
        """
        self.engine = db.create_engine(self.db_url)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()
        self.metadata.reflect(bind=self.engine)

        # Check and set table attribute
        if self.table_name in self.metadata.tables:
            self.table = self.metadata.tables[self.table_name]
        else:
            raise ValueError(f"Table {self.table_name} does not exist")

        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def count_logs(self) -> int:
        """
        Count the number of logs in the database using SQLAlchemy.
        Returns:
            int: The total number of logs in the specified table.
        """
        with self.engine.connect() as connection:
            # Use SQLAlchemy's expression language to create a query
            query = db.select(db.func.count()).select_from(self.table)
            result_proxy = connection.execute(query)
            count = result_proxy.scalar()  # Get the count directly
        return count

    def fetch_new_logs(self, batch_size: int):
        """
        Fetch new logs from the database since the last processed ID.

        Args:
            batch_size (int): The number of logs to fetch in one batch

        Returns:
            List of tuples containing log IDs and messages
        """
        query = f"""
            SELECT id, log_message
            FROM {self.table_name}
            WHERE id > {self.last_processed_id}
            ORDER BY id
            LIMIT {batch_size}
        """
        result = self.connection.execute(query).fetchall()
        if result:
            # Update last_processed_id to last ID in the result
            self.last_processed_id = result[-1][0]
        return result

    def close(self):
        """
        Close all database connections to prevent resource leaks.
        """
        if self.session:
            self.session.close()
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()

    def read_logs(self, start_id: int = 0, batch_size: int = 10000) -> List[Tuple]:
        """
        Read logs from the database starting from a specific ID.

        Args:
            start_id (int): The ID to start reading logs from
            batch_size (int): The number of logs to read in one batch

        Returns:
            List of tuples containing log IDs and messages
        """
        query = text(
            f"""
            SELECT id, log_message
            FROM {self.table_name}
            WHERE id > :start_id
            ORDER BY id
            LIMIT :batch_size
        """
        )
        params = {"start_id": start_id, "batch_size": batch_size}

        result_proxy = self.connection.execute(query, params)
        return result_proxy.fetchall()

    def switch_database(self, new_db_url: str, new_table_name: str):
        """
        Switch to a different database or table dynamically.

        Args:
            new_db_url (str): New database URL
            new_table_name (str): New table name
        """
        self.close()  # Close existing connections
        self.db_url = new_db_url
        self.table_name = new_table_name
        self._initialize_connection()


class Preprocessor:
    """
    Preprocesses log entries using regex-based pattern matching.
    Allows dynamic variable extraction and transformation.
    """

    def __init__(self, variables: List[str] = []):
        self.variables = self._parse_variables(variables)

    def _parse_variables(self, variables: List[str]) -> List[Tuple[str, re.Pattern]]:
        """
        Parse variable definitions into name-regex pairs.

        Example input: ["user:/^admin.*/", "ip:/\d+\.\d+\.\d+\.\d+/"]
        """
        parsed = []
        for var in variables:
            parts = var.split(":")
            if len(parts) <= 1:
                raise ValueError("Invalid variable format")
            name = parts[0]
            wrapped_regex = ":".join(parts[1:])
            regex = wrapped_regex.split("/")[1]
            parsed.append((name, re.compile(regex)))
        return parsed

    def process(self, fields: List[str]) -> List[Any]:
        """
        Process fields, replacing matched values with Variable objects.

        Args:
            fields (List[str]): Input log fields

        Returns:
            Processed fields with matched variables
        """
        if not self.variables:
            return fields

        result = []
        for field in fields:
            matched = False
            for name, regex in self.variables:
                if regex.match(field):
                    matched = Variable(name, field)
                    break
            result.append(matched if matched else field)
        return result


class Segmentator:
    """
    Helps in file segmentation for parallel processing.
    """

    def __init__(self, prefer_size: int = 1):
        self.prefer_size = prefer_size

    def _split_file(
        self, file_with_size: Tuple[str, int]
    ) -> List[Tuple[str, int, int, int]]:
        """
        Split a file into segments for parallel processing.

        Args:
            file_with_size (Tuple[filename, file_size])

        Returns:
            List of file segments with (filename, start, end, total_size)
        """
        filename, size = file_with_size
        n = self.prefer_size
        ranges = [(i * size // n, (i + 1) * size // n) for i in range(n)]
        return [(filename, r[0], r[1], size) for r in ranges]

    def create_segments(self, filenames: List[str]) -> List[Tuple[str, int, int, int]]:
        """
        Create file segments for multiple files.

        Args:
            filenames (List[str]): List of files to segment

        Returns:
            List of file segments
        """
        filename_with_sizes = [(f, os.stat(f).st_size) for f in filenames]
        result = []
        for f in filename_with_sizes:
            result.extend(self._split_file(f))
        return result


# Utility function
def size_of(filename: str) -> int:
    """Get file size in bytes."""
    return os.stat(filename).st_size


# Multi-threaded processing for fetching logs
def parallel_log_fetching(
    db_url: str, table_name: str, batch_size: int, num_workers: int = 4
):
    """
    Function to fetch logs in parallel using multiple workers.
    """
    log_reader = DatabaseLogReader(db_url, table_name)  # Initialize log reader
    ranges = [(i * batch_size, (i + 1) * batch_size) for i in range(num_workers)]

    # Use a thread pool to fetch logs in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        results = executor.map(lambda r: log_reader.fetch_new_logs(*r), ranges)

    # Collect all logs from the results
    logs = []
    for result in results:
        logs.extend(result)

    return logs  # Return the collected logs
