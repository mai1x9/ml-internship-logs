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
    def __init__(self, config, cluster_config, db_url='/home/ubuntu/logmine/logmine_pkg/HDFS_2K.db', table_name='log_table'):
        print("Initializing Processor...")
        self.cluster_config = cluster_config
        self.segmentator = Segmentator(multiprocessing.cpu_count())
        self.config = config
        print(f"Using database URL: {db_url}")
        print(f"Using table name: {table_name}")
        self.db_reader = DatabaseLogReader(f"sqlite:///{db_url}", table_name)
        print("Processor initialized.")

    def process(self, filenames):
        log("Processor: process filenames", filenames)
        # print("Processing filenames:", filenames)  # Debugging

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
            # print("Logs fetched from database:", logs)  # Debugging
            for log in logs:
                # print("Processing log:", log)  # Debugging
                clusterer.process_line(log[1])  # Adjust to your log structure
            start_id += batch_size  # Move to the next batch
        result = clusterer.result()
        # print("Database logs processing result:", result)  # Debugging
        return result



    def process_single_core(self, filenames):
        clusterer = Clusterer(**self.cluster_config)
        logs = self.db_reader.read_logs(0, 1000)  # Fetch logs from database
        # print("Logs fetched from database:", logs)  # Debugging
        for log in logs:
            # print("Processing log:", log)  # Debugging
            clusterer.process_line(log[1])  # Adjust to your log structure
        result = clusterer.result()
        # print("Single core processing result:", result)  # Debugging
        return result

    def process_pipe(self):
        clusterer = Clusterer(**self.cluster_config)
        try:
            for line in sys.stdin:
                # print("Processing line from pipe:", line)  # Debugging
                clusterer.process_line(line)
        except KeyboardInterrupt:
            pass
        finally:
            result = clusterer.result()
            # print("Pipe processing result:", result)  # Debugging
            return result

def map_segments_to_clusters(x):
    log('mapper: %s working on %s' % (os.getpid(), x))
    ((filename, start, end, size), config) = x
    clusterer = Clusterer(**config)
    db_reader = DatabaseLogReader('sqlite:///home/ubuntu/logmine/logmine_pkg/HDFS_2K.db', 'log_table')
    logs = db_reader.read_logs(start, size)
    # print("Logs fetched for mapping:", logs)  # Debugging
    clusters = clusterer.find([log[1] for log in logs])  # Adjust to your log structure
    # print("Clusters found:", clusters)  # Debugging
    return [(FIXED_MAP_JOB_KEY, clusters)]

def reduce_clusters(x):
    log('reducer: %s working on %s items' % (os.getpid(), len(x[0][1])))
    ((key, clusters_groups), config) = x
    # print("Clusters groups for reducing:", clusters_groups)  # Debugging
    if len(clusters_groups) <= 1:
        return (key, clusters_groups)  # Nothing to merge

    base_clusters = clusters_groups[0]
    merger = ClusterMerge(config)
    for clusters in clusters_groups[1:]:
        # print("Merging clusters:", clusters)  # Debugging
        merger.merge(base_clusters, clusters)
    result = (key, base_clusters)
    # print("Reduced clusters result:", result)  # Debugging
    return result