import os
import re
from dotenv import load_dotenv
from typing import List, Dict, Any

from .log_processing import Preprocessor
from .utils import LineScorer
from .vendor import water

# Load environment variables
load_dotenv()
DEFAULT_DB_URL = os.getenv("DB_URL")
DEFAULT_TABLE_NAME = os.getenv("TABLE_NAME")

FIXED_MAP_JOB_KEY = 1  # Constant for map-reduce operation


class PatternPlaceholder(str):
    """Placeholder for variations in log patterns."""

    pass


class PatternGenerator:
    """Generates generalized patterns from log entries."""

    def __init__(self, placeholder="---"):
        self.placeholder = placeholder  # Placeholder for variable parts in logs

    def create_pattern(self, a: List[str], b: List[str]) -> List[str]:
        """
        Generate a pattern by comparing two log entries.
        Replaces different parts with a placeholder.
        """
        if not a and not b:
            return []

        # Alignment function from vendor module
        (a, b) = water(a, b)

        # Replace differing parts with the placeholder
        return [
            a[i] if a[i] == b[i] else PatternPlaceholder(self.placeholder)
            for i in range(len(a))
        ]


class Clusterer:
    """
    Log clustering algorithm with configurable parameters.
    Groups similar log entries together.
    """

    def __init__(
        self,
        k1: float = 1,
        k2: float = 1,
        max_dist: float = 0.01,  # Maximum allowable distance between logs in a cluster
        variables: List[str] = [],
        delimeters: str = r"\s",
        min_members: int = 1,
        batch_size: int = 10000,  # Default batch size for processing logs
    ):
        self.pattern_generator = PatternGenerator()
        self.delimeters = delimeters
        self.preprocessor = Preprocessor(variables)
        self.scorer = LineScorer(k1, k2)
        self.max_dist = max_dist
        self.min_members = min_members
        self.batch_size = batch_size
        self.clusters = []

    def reset(self):
        """Reset the clusters."""
        self.clusters = []

    def process_line(self, line: str):
        """
        Process a single log line and cluster it.
        """
        tokens = re.split(self.delimeters, line.strip())
        processed_tokens = self.preprocessor.process(tokens)

        # Try to find a matching cluster
        found = False
        for cluster in self.clusters:
            representative, count, pattern = cluster
            score = self.scorer.distance(
                representative, processed_tokens, self.max_dist
            )

            if score <= self.max_dist:
                # If similar, update the cluster with the new log
                found = True
                cluster[1] += 1
                cluster[2] = self.pattern_generator.create_pattern(
                    pattern, processed_tokens
                )
                break

        if not found:
            # Create a new cluster if no match is found
            self.clusters.append([processed_tokens, 1, processed_tokens])

    def result(self) -> List[List[Any]]:
        """
        Return clusters, optionally filtering by minimum members.
        """
        return [cluster for cluster in self.clusters if cluster[1] >= self.min_members]

    def find(self, iterable_logs: List[str]) -> List[List[Any]]:
        """
        Find clusters in a list of log entries.
        """
        self.reset()  # Reset clusters before starting
        for line in iterable_logs:
            self.process_line(line)
        return self.result()


class ClusterMerge:
    """
    Merge clusters from different sources.
    """

    def __init__(self, config: Dict[str, Any]):
        self.clusterer = Clusterer(**config)  # Clusterer instance with configuration
        self.pattern_generator = self.clusterer.pattern_generator

    def merge(self, base_list: List[List[Any]], other_list: List[List[Any]]):
        """
        Merge clusters from another list into the base list.
        """
        for repr_a, count_a, pattern_a in other_list:
            exists = False
            for i, (repr_b, count_b, pattern_b) in enumerate(base_list):
                score = self.clusterer.scorer.distance(
                    repr_a, repr_b, self.clusterer.max_dist
                )
                if score <= self.clusterer.max_dist:
                    # Merge the cluster if similar
                    exists = True
                    base_list[i][1] += count_a
                    merged_pattern = self.pattern_generator.create_pattern(
                        pattern_a, pattern_b
                    )
                    base_list[i][2] = merged_pattern
                    break

            if not exists:
                # Add as a new cluster if no match is found
                base_list.append([repr_a, count_a, pattern_a])
