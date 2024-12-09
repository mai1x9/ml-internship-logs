import os
import sys
import multiprocessing
from multiprocessing import Pool
from dotenv import load_dotenv
from typing import Dict, Any

from .clustering import Clusterer, ClusterMerge
from .log_processing import DatabaseLogReader, Segmentator
from .utils import log, Output

# Load environment variables
load_dotenv()
DEFAULT_DB_URL = os.getenv("DB_URL")
DEFAULT_TABLE_NAME = os.getenv("TABLE_NAME")

FIXED_MAP_JOB_KEY = 1  # Constant for map-reduce operation


class Processor:
    """
    Processor for log clustering with multiple strategies.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        db_url: str = DEFAULT_DB_URL,
        table_name: str = DEFAULT_TABLE_NAME,
        batch_size: int = None,
    ):
        print("Initializing Processor...")
        # Calculate batch size dynamically if not provided
        batch_size = cluster_config.get("batch_size", batch_size)

        self.config = config
        self.db_reader = DatabaseLogReader(
            db_url, table_name
        )  # Reads logs from the database
        self.cluster_config = cluster_config
        self.segmentator = Segmentator(
            multiprocessing.cpu_count()
        )  # Utilizes multiple CPU cores for segmentation

        # Calculate total logs and adjust batch size dynamically
        total_logs = self.db_reader.count_logs()
        num_cores = multiprocessing.cpu_count()
        self.cluster_config["batch_size"] = batch_size or max(
            1, total_logs // num_cores
        )

        # Log initialization details
        print(f"Using database URL: {db_url}")
        print(f"Using table name: {table_name}")
        print(f"Total logs: {total_logs}")
        print(f"Number of cores: {num_cores}")
        print(f"Batch size set to: {self.cluster_config['batch_size']}")
        print("Processor initialized.")

    def switch_database(
        self, new_db_url: str = DEFAULT_DB_URL, new_table_name: str = DEFAULT_TABLE_NAME
    ):
        """
        Switch to a new database and table for processing.
        """
        print(f"Switching database to URL: {new_db_url}, Table: {new_table_name}")
        self.db_reader.switch_database(new_db_url, new_table_name)

    def process(self):
        """
        Process log data with different strategies.
        """
        log("Processor: process logs")

        # Use single-core processing if explicitly configured
        if self.config.get("single_core", False):
            return self.process_single_core()

        # Otherwise, use multi-core processing
        return self.process_multi_cores()

    def process_database_logs(self):
        """
        Process logs from the database using a single clusterer instance.
        """
        clusterer = Clusterer(**self.cluster_config)
        start_id = 0

        # Read and process logs in batches
        while True:
            logs = self.db_reader.read_logs(start_id, self.cluster_config["batch_size"])
            if not logs:
                break

            # Process each log entry
            for log_entry in logs:
                clusterer.process_line(log_entry[1])
            start_id += self.cluster_config["batch_size"]

        print("process_db is being called")
        return clusterer.result()

    def process_single_core(self):
        """
        Process logs from the database on a single core.
        """
        batch_size = self.cluster_config["batch_size"]
        print(f"Processing logs with batch size: {batch_size}")

        clusterer = Clusterer(**self.cluster_config)
        start_id = 0

        # Process logs batch by batch
        while True:
            logs = self.db_reader.read_logs(start_id, self.cluster_config["batch_size"])
            if not logs:
                break
            for log_entry in logs:
                clusterer.process_line(log_entry[1])
            start_id += self.cluster_config["batch_size"]

        print("Single-core processing completed.")
        return clusterer.result()

    @staticmethod
    def process_chunk(start_id, batch_size, db_reader_config, cluster_config):
        """
        Process a chunk of logs from the database.

        Args:
            start_id (int): Starting ID for logs in the database.
            batch_size (int): Number of logs to process in this chunk.
            db_reader_config (dict): Configuration for DatabaseLogReader.
            cluster_config (dict): Configuration for the Clusterer.

        Returns:
            List: Cluster results for this chunk.
        """
        # Reinitialize database reader and clusterer in each process
        db_reader = DatabaseLogReader(**db_reader_config)
        clusterer = Clusterer(**cluster_config)

        # Read the log chunk
        logs = db_reader.read_logs(start_id, batch_size)

        # Process each log entry in the chunk
        for log_entry in logs:
            clusterer.process_line(log_entry[1])

        db_reader.close()  # Ensure the database connection is closed in each process
        return clusterer.result()

    def process_multi_cores(self):
        """
        Process logs from the database using multiprocessing.
        """
        # Configurations to pass to the worker function
        db_reader_config = {
            "db_url": self.db_reader.db_url,
            "table_name": self.db_reader.table_name,
        }
        cluster_config = self.cluster_config

        # Get the total number of logs and divide them into chunks
        batch_size = self.cluster_config["batch_size"]
        total_logs = self.db_reader.count_logs()
        start_ids = range(0, total_logs, batch_size)

        print(f"Dividing {total_logs} logs into chunks of {batch_size}...")
        print(
            f"Starting multi-core processing with {multiprocessing.cpu_count()} cores."
        )

        # Use multiprocessing pool to process chunks parallely
        with Pool(processes=multiprocessing.cpu_count()) as pool:
            results = pool.starmap(
                self.process_chunk,
                [
                    (start_id, batch_size, db_reader_config, cluster_config)
                    for start_id in start_ids
                ],
            )

        # Merge the results from each process
        final_clusters = []
        for result in results:
            final_clusters.extend(result)

        print("Multi-core processing completed.")
        return final_clusters

    def process_pipe(self):
        """
        Process logs from standard input.
        """
        clusterer = Clusterer(**self.cluster_config)
        try:
            for line in sys.stdin:
                clusterer.process_line(line)
        except KeyboardInterrupt:
            pass
        return clusterer.result()


class LogMine:
    """
    Main log mining and processing class.
    """

    def __init__(
        self,
        processor_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        output_options: Dict[str, Any],
        output_file_path: str = None,  # Add an optional file path for output
    ):
        log(
            "LogMine: init with config:",
            processor_config,
            cluster_config,
            output_options,
        )
        self.processor = Processor(processor_config, cluster_config)
        self.output = Output(output_options)

        # Set the output file if provided
        self.output_file_path = output_file_path
        if self.output_file_path:
            self.output.set_output_file(self.output_file_path)

    def run(self):
        """
        Run log mining process on given files.
        """
        log("LogMine: run with files:")
        clusters = self.processor.process()  # Process logs and get clusters
        log("LogMine: output cluster:", clusters)

        # Output clusters to the specified file
        self.output.out(clusters)

        # Close the output file if it's being used
        if self.output_file_path:
            self.output.close_output_file()


# Map-Reduce Functions
def map_segments_to_clusters(x):
    """
    Map function for distributed log clustering.

    Args:
        x: Tuple containing file segment details and configuration.

    Returns:
        Mapped clusters for a log segment.
    """
    log("mapper: %s working on %s" % (os.getpid(), x))
    ((filename, start, end, size), config) = x

    # Initialize clusterer and database reader
    clusterer = Clusterer(**config)
    db_reader = DatabaseLogReader(DEFAULT_DB_URL, DEFAULT_TABLE_NAME)

    # Read logs for the specified segment
    logs = db_reader.read_logs(start, size)

    # Cluster logs and return results
    clusters = clusterer.find([log[1] for log in logs])  # Adjust for log structure
    return [(FIXED_MAP_JOB_KEY, clusters)]


def reduce_clusters(x):
    """
    Reduce function for merging clusters.

    Args:
        x: Tuple containing the key and grouped clusters.

    Returns:
        Merged clusters after reduction.
    """
    log("reducer: %s working on %s items" % (os.getpid(), len(x[0][1])))
    ((key, clusters_groups), config) = x

    # If only one group, no merging needed
    if len(clusters_groups) <= 1:
        return (key, clusters_groups)

    # Merge clusters from all groups
    base_clusters = clusters_groups[0]
    merger = ClusterMerge(config)

    for clusters in clusters_groups[1:]:
        merger.merge(base_clusters, clusters)

    return (key, base_clusters)
