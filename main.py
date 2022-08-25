import logging
import os
import sys
import json
import yaml
import re
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


def read_query(query: str) -> set:
    """
    Reads query string with a simple regex match and cleans it up a bit before processing.
    Returns a set (no duplicates) of table names found in query.

    example matches:
        FROM project.dataset.table
        from `project.dataset.table`
        JOIN `project.dataset.table`
        join project.dataset.table

    :param query: string with query
    :return: set of table names (format of table name: project.dataset.table)
    """

    # strip extra whitespace
    query_lines = [line.strip() for line in query.split('\n')]

    # remove empty strings and comments
    cleaned_query = ' '.join([line for line in query_lines if line and not line.startswith('--')])

    # see example matches above
    matches = re.findall(
        pattern=r'(FROM|from|JOIN|join){1} `+([a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+)`+',
        string=cleaned_query
    )

    return set([groups[1] for groups in matches])


def parse_scheduled_queries() -> dict:
    """
    Retrieves all scheduled queries from GCP project and parses them for tables used.
    Assumes you have BigQuery CLI setup and sufficient permissions to read project's scheduled queries

    :return: Dict<table_name: List[scheduled_query_names]> Dictionary with scheduled queries associated with each table
    """

    table_dict = defaultdict(list)

    # retrieves scheduled queries, returns JSON collection
    response = run(
        f"bq ls --project_id={os.getenv('GCP_PROJECT')} --transfer_config --transfer_location=EU --format=prettyjson",
        stdout=PIPE,
        stderr=PIPE,
        universal_newlines=True,
        shell=True
    )

    # run through collection and parse queries
    for scheduled_query in json.loads(response.stdout):

        query = scheduled_query['params'].get('query')

        for tablename in read_query(query):
            table_dict[tablename].append(scheduled_query.get('displayName'))

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

                if filename.endswith('.sql') or filename.endswith('.bigquery'):

                    with open(os.path.join(root, filename)) as sql_file:
                        for tablename in read_query(sql_file.read()):
                            proj_table_dict[tablename].add(proj)

    return {table_name: list(proj_lst) for table_name, proj_lst in proj_table_dict.items()}


if __name__ == "__main__":
    proj_res = parse_projects()
    sq_res = parse_scheduled_queries()

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
