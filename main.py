import logging
import os
import sys
import json
from typing import Iterable, Callable

import yaml
import re
import argparse
from subprocess import PIPE, run
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
    logging.info("Environment GOOGLE_APPLICATION_CREDENTIALS is present\n")
else:
    logging.fatal(
        "Environment GOOGLE_APPLICATION_CREDENTIALS not found."
        "Aborting execution!")
    sys.exit(1)


TABLE_REGEX = r'`?([a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+)`?'


def read_file(root: str, filename: str, parser_func: Callable) -> Iterable:
    """
    Read file contents and parses it according to provided method, returning tables found within.

    :param root: directory path
    :param filename: name of file
    :param parser_func: parser function to be called on file contents
    :return: Iterable (set/list) of table names found in file contents
    """

    with open(os.path.join(root, filename)) as input_file:
        return parser_func(input_file.read().splitlines())


def parse_query(query: list) -> set:
    """
    Reads query string with a simple regex match and cleans it up a bit before processing.
    Returns a set (no duplicates) of table names found in query.

    example matches:
        FROM project.dataset.table
        from `project.dataset.table`
        JOIN `project.dataset.table`
        join project.dataset.table

    :param query: list with query lines
    :return: set of table names (format of table name: project.dataset.table)
    """

    # strip extra whitespace
    query_lines = [line.strip() for line in query]

    # remove empty strings and comments
    cleaned_query = ' '.join([line for line in query_lines if line and not line.startswith('--')])

    # see example matches above
    matches = re.findall(
        pattern=r'(FROM|from|JOIN|join){1} ' + TABLE_REGEX,
        string=cleaned_query
    )

    return set([groups[1] for groups in matches])


def parse_settings(settings: list):
    """
    Parse settings.py files to find tables mentioned within them.
    :param settings: list of strings containing lines from settings.py files
    :return: list of table names found in settings.py file
    """

    # cleanup lines
    settings_lines = [line.strip().lower() for line in settings if line.strip()]

    # try to find potential candidates based on certain keywords and that are not commented out
    candidates = [
        re.findall(TABLE_REGEX, line) for line in settings_lines
        if not line.startswith("#") and re.search(r'bq|bigquery|table', line.lower())
    ]

    # format results into contiguous list and cleans up false positives such as function calls
    return [
        candidate for candidate_list in candidates for candidate in candidate_list
        if candidate and not candidate.startswith(('os.', 'sys.'))
    ]


def parse_scheduled_queries() -> dict:
    """
    Retrieves all scheduled queries from GCP project and parses them for tables used.
    Assumes you have BigQuery CLI setup and sufficient permissions to read project's scheduled queries

    :return: Dict<table_name: List[scheduled_query_names]> Dictionary with scheduled queries associated with each table
    """

    table_dict = defaultdict(list)

    bq_command = (
        "bq ls",
        f"--application_default_credential_file={os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}",
        f"--project_id={os.getenv('GCP_PROJECT')}",
        "--transfer_config",
        "--transfer_location=EU",
        "--format=prettyjson"
    )

    # retrieves scheduled queries, returns JSON collection
    response = run(
        " ".join(bq_command),
        stdout=PIPE,
        stderr=PIPE,
        universal_newlines=True,
        shell=True
    )

    # run through collection and parse queries
    for scheduled_query in json.loads(response.stdout):

        query = scheduled_query['params'].get('query')

        query_name = scheduled_query.get('displayName') + (' (disabled)' if scheduled_query.get('disabled') else '')

        for tablename in parse_query(query.split('\n')):
            table_dict[tablename].append(query_name)

    return dict(table_dict)


def parse_projects():
    """
    Explores project directory and finds queries in them.
    Assumes queries have file extensions .sql or .bigquery

    :return: Dict<table_name: List[project_names]> Dictionary with projects associated with each table
    """

    # different query files can have the same table, prevent duplicates by defaulting dict values to a set
    proj_table_dict = defaultdict(set)

    # it is possible to exclude certain directories
    with open('ignore_dirs.txt', 'r') as ignorefile:
        ignore = ignorefile.read().splitlines()

    # get all high-level project directories
    proj_root = os.getenv('PROJ_ROOT')
    project_dirs = [proj for proj in os.listdir(proj_root) if proj not in ignore]

    for proj in project_dirs:
        for root, dirs, files in os.walk(os.path.join(proj_root, proj)):
            for filename in files:

                proj_tables = []

                if filename.endswith('.sql') or filename.endswith('.bigquery'):
                    proj_tables.extend(read_file(root, filename, parse_query))

                if filename == 'settings.py':
                    proj_tables.extend(read_file(root, filename, parse_settings))

                for tablename in set(proj_tables):
                    proj_table_dict[tablename].add(proj)

    return {table_name: list(proj_lst) for table_name, proj_lst in proj_table_dict.items()}


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='BigQuery Table Parser')

    parser.add_argument('-p', '--projects', dest='only_projs', action='store_true', required=False,
                        help='only check projects')
    parser.add_argument('-q', '--queries', dest='only_queries', action='store_true', required=False,
                        help='only check queries')

    args = parser.parse_args()

    # checks if user only wants to parse certain contents
    proj_res = parse_projects() if not args.only_queries else dict()
    sq_res = parse_scheduled_queries() if not args.only_projs else dict()

    total_dict = {}
    total_tables = list(proj_res.keys()) + list(sq_res.keys())

    # alphabetically sorts both keys(tables) and values(usages)
    for table in sorted(set(total_tables)):

        total_dict[table] = {}

        # separate queries from code usage for better readability
        if table in sq_res:
            total_dict[table]["queries"] = sorted(sq_res.get(table))
        if table in proj_res:
            total_dict[table]["code"] = sorted(proj_res.get(table))

    with open('result.yaml', 'w') as file:
        file.write(yaml.dump(total_dict))
