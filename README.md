# bigquery-table-parser
Parses scheduled queries and project directories trying to find all tables used within them.

The goal of having this collection is to facilitate efforts when trying to find specific tables being used. One example 
would be if a table is going to be deprecated/altered and you need to find all references to it in order to more quickly
patch the issue.

Note: while the initial goal of this script is finding BigQuery tables, other SQL tables might be found provided that 
they follow the same structure being scanned for (project.dataset.table).

## Usage

The script that generates the YAML presented below scrapes all scheduled queries and files
with a `.sql` or `.bigquery` extension. The script will also try to find tables within `settings.py` files.

### .env
Use the provided `.env` example to fill in your environment variables. 
Once these are set, remove the *.example* termination.

Note: the script assumes all projects are within the same root folder (`PROJ_ROOT`). 

### ignore_dirs
Similar to a git ignore file, use the provided `ignore_dirs.txt` file to add directories within your `PROJ_ROOT` 
(defined earlier as an environment variable) that you wish to not parse.

### main.py arguments
The script allows the user to specify whether to parse only scheduled queries or project directories. Default is both.

`-p` or `--projects` will tell the script to only parse your project directory

`-q` or `--queries` will tell the script to only parse your scheduled queries

## Examples

```
python main.py

python main.py -p

python main.py --queries
```


