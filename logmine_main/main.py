import sys
import os
import time
import multiprocessing
from dotenv import load_dotenv
from .processing import LogMine
from .config import Input

# Load environment variables from a .env file
load_dotenv()


def run():
    start_time = time.time()  # Record the start time for performance measurement

    inp = Input()  # Initialize the Input class

    # Check if there are any arguments passed via the command line or stdin
    if len(sys.argv) == 1 and sys.stdin.isatty():
        inp.print_help()  # Print help message if no arguments are passed
        return

    # Get the arguments from input
    options = inp.get_args()

    # Adjust options if output is not a terminal (e.g., when redirecting to a file)
    if not sys.stdout.isatty():
        options["highlight_patterns"] = False
        options["highlight_variables"] = False

    output_file_path = os.getenv(
        "output_path"
    )  # Get the output file path from environment variables

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    # Calculate batch_size if not provided or invalid
    if "batch_size" not in options or options["batch_size"] <= 0:
        # Temporary LogMine instance to get total logs
        logmine_temp = LogMine(
            {k: options[k] for k in ("single_core",)},  # Processor config
            {
                k: options[k]
                for k in (
                    "max_dist",
                    "variables",
                    "delimeters",
                    "min_members",
                    "k1",
                    "k2",
                )
            },  # Clustering config
            {},
        )
        total_logs = logmine_temp.processor.db_reader.count_logs()  # Count total logs
        num_cores = multiprocessing.cpu_count()  # Get the number of CPU cores

        # Calculate batch size based on total logs and number of cores
        if total_logs < num_cores * 10000:
            options["batch_size"] = max(
                1, total_logs // num_cores
            )  # Distribute logs evenly across cores
        else:
            options["batch_size"] = (
                10000  # Default batch size set to 10,000 for large files
            )

        print(
            f"Calculated batch size: {options['batch_size']}"
        )  # Output the calculated batch size

    # Create LogMine instance with updated batch_size
    logmine = LogMine(
        {k: options[k] for k in ("single_core",)},  # Processor config
        {
            k: options[k]
            for k in (
                "max_dist",
                "variables",
                "delimeters",
                "min_members",
                "k1",
                "k2",
                "batch_size",
            )
        },  # Clustering config
        {
            k: options[k]
            for k in (
                "sorted",
                "number_align",
                "pattern_placeholder",
                "mask_variables",
                "highlight_patterns",
                "highlight_variables",
            )
        },  # Output options
    )

    # Set the output file for LogMine
    logmine.output.set_output_file(output_file_path)

    # Run LogMine and get the result
    result = logmine.run()

    end_time = time.time()  # Record the end time
    total_time = end_time - start_time  # Calculate total processing time

    # Display processing time statistics
    if total_time < 60:
        print(f"\n--- Processing Time Statistics ---")
        print(f"Total Time Taken: {total_time:.2f} seconds")
    else:
        minutes, seconds = divmod(total_time, 60)
        print(f"Total Time: {int(minutes)} minutes and {seconds:.2f} seconds")

    return result


if __name__ == "__main__":
    run()  # Execute the run function if this script is run directly
