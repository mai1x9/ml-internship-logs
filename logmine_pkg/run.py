import sys
from .log_mine import LogMine
from .cli_input import Input

def run():
    # print("Initializing Input...")  # Debugging
    inp = Input()

    # print("Checking for arguments...")  # Debugging
    if len(sys.argv) == 1 and sys.stdin.isatty():
        inp.print_help()
        return

    # print("Getting arguments...")  # Debugging
    options = inp.get_args()
    # print("Options obtained:", options)  # Debugging

    if not sys.stdout.isatty():
        options['highlight_patterns'] = False
        options['highlight_variables'] = False

    # Confirm the database URL and table name are correct
    db_url = '/home/ubuntu/logmine/logmine_pkg/OpenSSH_2k.db'
    table_name = 'log_table'
    # print(f"Database URL: sqlite:///{db_url}")  # Debugging
    # print(f"Table Name: {table_name}")  # Debugging

    logmine = LogMine(
        {k: options[k] for k in ('single_core',)},
        {k: options[k] for k in ('max_dist', 'variables', 'delimeters', 'min_members', 'k1', 'k2')},
        {k: options[k] for k in ('sorted', 'number_align', 'pattern_placeholder', 'mask_variables', 'highlight_patterns', 'highlight_variables')}
    )

    # print("Running LogMine...")  # Debugging
    result = logmine.run([])
    # print("Result:", result)  # Debugging
    return result

if __name__ == "__main__":
    run()
