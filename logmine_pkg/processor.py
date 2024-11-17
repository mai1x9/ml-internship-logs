import sys
import os
import multiprocessing
from .clusterer import Clusterer
from .cluster_merge import ClusterMerge
from .database_reader import DatabaseLogReader
from .map_reduce import MapReduce
from .segmentator import Segmentator
from .debug import log

FIXED_MAP_JOB_KEY = 1  # Single key for the whole map-reduce operation

class Processor():
    def __init__(self, config, cluster_config, db_url='/home/ubuntu/logmine/logmine_pkg/OpenSSH_2k.db', table_name='log_table'):
        print("Initializing Processor...")
        self.cluster_config = cluster_config
        self.segmentator = Segmentator(multiprocessing.cpu_count())
        self.config = config
        self.db_reader = DatabaseLogReader(f"sqlite:///{db_url}", table_name)
        print(f"Using database URL: {db_url}")
        print(f"Using table name: {table_name}")
        print("Processor initialized.")

    def switch_database(self, new_db_url='/home/ubuntu/logmine/logmine_pkg/OpenSSH_2k.db', new_table_name='log_table'):
        """Switch to a new database and table for processing."""
        print(f"Switching database to URL: {new_db_url}, Table: {new_table_name}")
        self.db_reader.switch_database(new_db_url, new_table_name)

    def process(self, filenames):
        log("Processor: process filenames", filenames)

        if not filenames or filenames == ['-']:
            return self.process_database_logs()

        if self.config.get('single_core'):
            return self.process_single_core(filenames)

        return self.process_multi_cores(filenames)

    def process_database_logs(self):
        clusterer = Clusterer(**self.cluster_config)
        start_id = 0
        batch_size = 1000
        while True:
            logs = self.db_reader.read_logs(start_id, batch_size)
            if not logs:
                break  # Exit loop if no more logs are found
            for log in logs:
                clusterer.process_line(log[1])  # Adjust to your log structure
            start_id += batch_size  # Move to the next batch
        result = clusterer.result()
        return result

    def process_single_core(self, filenames):
        clusterer = Clusterer(**self.cluster_config)
        logs = self.db_reader.read_logs(0, 1000)  # Fetch logs from database
        for log in logs:
            clusterer.process_line(log[1])  # Adjust to your log structure
        result = clusterer.result()
        return result

    def process_pipe(self):
        clusterer = Clusterer(**self.cluster_config)
        try:
            for line in sys.stdin:
                clusterer.process_line(line)
        except KeyboardInterrupt:
            pass
        finally:
            result = clusterer.result()
            return result

def map_segments_to_clusters(x):
    log('mapper: %s working on %s' % (os.getpid(), x))
    ((filename, start, end, size), config) = x
    clusterer = Clusterer(**config)
    db_reader = DatabaseLogReader('sqlite:///home/ubuntu/logmine/logmine_pkg/OpenSSH_2k.db', 'log_table')
    logs = db_reader.read_logs(start, size)
    clusters = clusterer.find([log[1] for log in logs])  # Adjust to your log structure
    return [(FIXED_MAP_JOB_KEY, clusters)]

def reduce_clusters(x):
    log('reducer: %s working on %s items' % (os.getpid(), len(x[0][1])))
    ((key, clusters_groups), config) = x
    if len(clusters_groups) <= 1:
        return (key, clusters_groups)  # Nothing to merge

    base_clusters = clusters_groups[0]
    merger = ClusterMerge(config)
    for clusters in clusters_groups[1:]:
        merger.merge(base_clusters, clusters)
    result = (key, base_clusters)
    return result
