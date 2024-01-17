from pathlib import Path
from typing import Optional
import json
import logging

import psycopg2
import pandas as pd
import sqlalchemy

from pipeline.helpers.config import config
from pipeline.helpers import utils


def handle_null(query: str) -> str:
    """
    Replaces all occurrences of the string 'NULL' with the SQL NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'NULL' replaced with NULL.
    """
    query = query.replace("'NULL'", "NULL")

    return query


def handle_nan(query: str) -> str:
    """
    Replaces all occurrences of the string 'nan' with the SQL NULL keyword in the given query.

    Args:
        query (str): The SQL query to modify.

    Returns:
        str: The modified SQL query with 'nan' replaced with NULL.
    """
    query = query.replace("'nan'", "NULL")

    return query


def santize_string(string: str) -> str:
    """
    Sanitizes a string by escaping single quotes.

    Args:
        string (str): The string to sanitize.

    Returns:
        str: The sanitized string.
    """
    return string.replace("'", "''")


def sanitize_json(json_dict: dict) -> str:
    """
    Sanitizes a JSON object by replacing single quotes with double quotes.

    Args:
        json_dict (dict): The JSON object to sanitize.

    Returns:
        str: The sanitized JSON object.
    """
    for key, value in json_dict.items():
        if type(value) is str:
            json_dict[key] = santize_string(value)
    return json.dumps(json_dict)


def execute_queries(
    config_file: Path,
    queries: list,
    show_commands=True,
    show_progress=False,
    silent=False,
    logger: Optional[logging.Logger] = None,
) -> list:
    """
    Executes a list of SQL queries on a PostgreSQL database.

    Args:
        config_file_path (str): The path to the configuration file containing the connection parameters.
        queries (list): A list of SQL queries to execute.
        show_commands (bool, optional): Whether to display the executed SQL queries. Defaults to True.
        show_progress (bool, optional): Whether to display a progress bar. Defaults to False.
        silent (bool, optional): Whether to suppress output. Defaults to False.

    Returns:
        list: A list of tuples containing the results of the executed queries.
    """

    if logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

    conn = None
    command = None
    output = []
    try:
        # read the connection parameters
        params = config(path=config_file, section="postgresql")
        # connect to the PostgreSQL server
        if show_commands:
            logger.debug(
                "[grey]Connecting to the PostgreSQL database...", extra={"markup": True}
            )
            logger.debug(
                f"[grey]{params['host']}:{params['port']} {params['database']} ({params['user']})",
                extra={"markup": True},
            )

        conn = psycopg2.connect(**params)  # type: ignore
        cur = conn.cursor()

        def execute_query(query: str):
            if show_commands:
                logger.debug("Executing query:")
                logger.debug(f"[bold blue]{query}", extra={"markup": True})
            cur.execute(query)
            try:
                output.append(cur.fetchall())
            except psycopg2.ProgrammingError:
                pass

        if show_progress:
            with utils.get_progress_bar() as progress:
                task = progress.add_task("Executing SQL queries...", total=len(queries))

                for command in queries:
                    progress.update(task, advance=1)
                    execute_query(command)

        else:
            for command in queries:
                execute_query(command)

        # close communication with the PostgreSQL database server
        cur.close()

        # commit the changes
        conn.commit()

        if not silent:
            logger.debug(
                f"[grey]Executed {len(queries)} SQL query(ies).", extra={"markup": True}
            )
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error("[bold red]Error executing queries.", extra={"markup": True})
        if command is not None:
            logger.error(f"[red]For query: {command}", extra={"markup": True})
        logger.error(e)
        raise e
    finally:
        if conn is not None:
            conn.close()

    return output


def get_db_connection(config_file: Path) -> sqlalchemy.engine.base.Engine:
    """
    Establishes a connection to the PostgreSQL database using the provided configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        sqlalchemy.engine.base.Engine: The database connection engine.
    """
    params = config(path=config_file, section="postgresql")
    engine = sqlalchemy.create_engine(
        "postgresql+psycopg2://"
        + params["user"]
        + ":"
        + params["password"]
        + "@"
        + params["host"]
        + ":"
        + params["port"]
        + "/"
        + params["database"]
    )

    return engine


def execute_sql(config_file: Path, query: str) -> pd.DataFrame:
    """
    Executes a SQL query on a PostgreSQL database and returns the result as a pandas DataFrame.

    Args:
        config_file_path (str): The path to the configuration file containing the PostgreSQL database credentials.
        query (str): The SQL query to execute.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the result of the SQL query.
    """
    engine = get_db_connection(config_file=config_file)

    df = pd.read_sql(query, engine)

    engine.dispose()

    return df


def fetch_record(config_file: Path, query: str) -> Optional[str]:
    """
    Fetches a single record from the database using the provided SQL query.

    Args:
        config_file_path (str): The path to the database configuration file.
        query (str): The SQL query to execute.

    Returns:
        Optional[str]: The value of the first column of the first row of the result set,
        or None if the result set is empty.
    """
    df = execute_sql(config_file=config_file, query=query)

    # Check if there is a row
    if df.shape[0] == 0:
        return None

    value = df.iloc[0, 0]

    return str(value)
