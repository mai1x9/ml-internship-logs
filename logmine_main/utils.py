import os
import functools
import sys
import signal
import collections
import itertools
import multiprocessing
from .vendor import water


# Logging function
def log(*args):
    if "VERBOSE" in os.environ:
        print(args)


# Placeholders and pattern generation
class PatternPlaceholder(str):
    pass


class PatternGenerator:
    def __init__(self, placeholder="---"):
        self.placeholder = placeholder

    def create_pattern(self, a, b):
        if len(a) == 0 and len(b) == 0:
            return []
        (a, b) = water(a, b)
        new = []
        for i in range(len(a)):
            if a[i] == b[i]:
                new.append(a[i])
            else:
                new.append(PatternPlaceholder(self.placeholder))
        return new


# Variable class for pattern matching
class Variable:
    def __init__(self, value, name=None):
        self.value = value
        self.name = name or value

    def __eq__(self, other):
        if other is None:
            return False
        if isinstance(other, str):
            return self.value == other
        return self.value == other.value

    def __repr__(self):
        return "'%s'" % self.value

    def __str__(self):
        return self.value

    def __add__(self, other):
        return self.value + other

    def __radd__(self, other):
        return other + self.value


# Constants for colored output
CRED = "\33[31m"
CYELLOW = "\33[33m"
CEND = "\033[0m"


# Output Class
class Output:
    def __init__(self, options):
        self.options = options
        self.output_file = None  # Explicitly set this to None initially

    def set_output_file(self, file_path):
        """
        Set the output file path. If the file exists, it will be overwritten.
        """
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        self.output_file = open(file_path, "w")  # Open file in write mode

    def close_output_file(self):
        """
        Close the output file, if open.
        """
        if self.output_file:
            self.output_file.close()
            self.output_file = None

    def out(self, clusters):
        """
        Output the clusters to the specified file.
        """
        if not self.output_file:
            raise ValueError(
                "Output file not set. Use 'set_output_file' to specify the file."
            )

        log("Output: out", clusters)
        if len(clusters) == 0:
            return

        # Sorting
        if self.options.get("sorted") == "desc":
            sort_func = functools.cmp_to_key(lambda x, y: y[1] - x[1])
            clusters = sorted(clusters, key=sort_func)
        if self.options.get("sorted") == "asc":
            sort_func = functools.cmp_to_key(lambda x, y: x[1] - y[1])
            clusters = sorted(clusters, key=sort_func)

        # Alignment
        if self.options.get("number_align") is True:
            width = max([len(str(c[1])) for c in clusters])
        else:
            width = 0

        # Write clusters to file
        for [fields, count, pattern] in clusters:
            subject = []
            output = []

            if len(fields) != len(pattern):
                subject = pattern  # Fallback to pattern
            else:
                subject = fields

            for i in range(len(subject)):
                field = subject[i]
                if isinstance(pattern[i], PatternPlaceholder):
                    placeholder = self.options.get("pattern_placeholder")
                    if placeholder is None:
                        value = field
                    else:
                        value = placeholder
                    if self.options.get("highlight_patterns") is True:
                        value = CRED + value + CEND
                    output.append(value)
                elif isinstance(pattern[i], Variable):
                    if self.options.get("mask_variables") is True:
                        value = str(field)
                    else:
                        value = field.name
                    if self.options.get("highlight_variables") is True:
                        value = CYELLOW + value + CEND
                    output.append(value)
                else:
                    output.append(field)

            # Write the formatted line to the file
            formatted_line = "%s %s\n" % (
                str(count).rjust(width),
                " ".join(str(_) for _ in output),
            )
            self.output_file.write(formatted_line)

        log("Output written to file successfully.")

    def __del__(self):
        """
        Ensure the file is closed when the object is deleted.
        """
        self.close_output_file()


# MapReduce class
# In case the program use multiple MapReduce instances, we ensure there will
# always be 1 pool used. Too many pools created will cause "Too many open
# files" error.
STATIC_POOL = [None]


class MapReduce:

    def __init__(self, map_func, reduce_func, params=None):
        """
        map_func

          Function to map inputs to intermediate data. Takes as
          argument one input value and returns a tuple with the
          key and a value to be reduced.

        reduce_func

          Function to reduce partitioned version of intermediate
          data to final output. Takes as argument a key as
          produced by map_func and a sequence of the values
          associated with that key.

        num_workers

          The number of workers to create in the pool. Defaults
          to the number of CPUs available on the current host.
        """
        if STATIC_POOL[0] is None:
            # Disable SIGINT handler in all processes in the pool
            # This helps terminate the whole pool when user press Ctrl + C
            original_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
            STATIC_POOL[0] = multiprocessing.Pool()
            signal.signal(signal.SIGINT, original_handler)
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = STATIC_POOL[0]
        self.params = params

    def dispose(self):
        self.pool.close()
        STATIC_POOL[0] = None

    def partition(self, mapped_values):
        """Organize the mapped values by their key.
        Returns an unsorted sequence of tuples with a key
        and a sequence of values.
        """
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        """Process the inputs through the map and reduce functions
        given.

        inputs
          An iterable containing the input data to be processed.

        chunksize=1
          The portion of the input data to hand to each worker.
          This can be used to tune performance during the mapping
          phase.
        """
        map_inputs = inputs
        if self.params is not None:
            map_inputs = zip(inputs, [self.params] * len(inputs))

        try:
            map_responses = self.pool.map(
                self.map_func,
                map_inputs,
                chunksize=chunksize,
            )
            # TODO: Partitions balancing?
            partitioned_data = self.partition(itertools.chain(*map_responses))

            reduce_inputs = partitioned_data
            if self.params is not None:
                count = len(partitioned_data)
                reduce_inputs = zip(partitioned_data, [self.params] * count)

            reduced_values = self.pool.map(self.reduce_func, reduce_inputs)
            return reduced_values
        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, terminating processes")
            self.pool.terminate()
            self.pool.join()


# Line scorer for comparing fields
class LineScorer:
    def __init__(self, k1, k2):
        self.k1 = k1
        self.k2 = k2
        self.early_returned = False

    def distance(self, fields1, fields2, max_dist=None):
        if not (isinstance(fields1, list) and isinstance(fields2, list)):
            raise TypeError("Fields must be a list")

        max_len = max(len(fields1), len(fields2))
        min_len = min(len(fields1), len(fields2))

        total = 0
        for i in range(min_len):
            total += 1.0 * self.score(fields1[i], fields2[i]) / max_len

            if max_dist is not None and (1 - total) < max_dist:
                self.early_returned = True
                return 1 - total

        self.early_returned = False
        return 1 - total

    def score(self, field1, field2):
        if isinstance(field1, str) and isinstance(field2, str) and field1 == field2:
            return self.k1

        if (
            isinstance(field1, Variable)
            and isinstance(field2, Variable)
            and field1 == field2
        ):
            return self.k2

        return 0
