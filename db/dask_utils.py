import logging
import datetime
from typing import List
import os

import sqlalchemy as sa
from sqlalchemy.engine import Engine
import dask.dataframe as dd
import pandas as pd

from db.utils import (
    DATABASE_URI, 
    extract_table_columns
)


FilePaths = List[str]

logger = logging.getLogger(__name__)


def python_dt_type_to_numpy(python_type: type) -> type:
    """Convert any datetime types to numpy equivalents. 

    Some Dask/Pandas functionality only works with numpy datetime
    types, not the native Python versions. 
    """
    dt_types = [datetime.date, datetime.datetime]
    return 'datetime64[ns]' if python_type in dt_types else python_type


def extract_dask_meta(
    table: sa.Table, 
    index_col: str = None
) -> pd.DataFrame:
    """Extract metadata (typing info) from a SQLAlchemy model 
    into a format that Dask understands. 

    Args:
        model: A SQLAlchemy declarative model for a table
        idx_col: Index column for incoming data, if any. Will need
            to be removed from meta. 

    Returns: 
        An empty DataFrame with defined types
    """
    meta_dict = {
        c.name: python_dt_type_to_numpy(c.type.python_type)
        for c in table.columns
    }
    df: pd.DataFrame = dd.utils.make_meta(meta_dict)

    if index_col:
        df = df.drop(index_col, axis='columns')
    
    return df


def ddf_from_table(
    table: sa.Table,
    index_col: str,
    custom_query: sa.sql.Selectable = None,
    npartitions: int = None
) -> dd.DataFrame:
    query = sa.select(table) if custom_query is None else custom_query
    types_meta = extract_dask_meta(table, index_col=index_col)
    return dd.read_sql_query(
        sql=query,
        con=DATABASE_URI,
        index_col=index_col,
        meta=types_meta,
        npartitions=npartitions
    )


def rm_cmd(rm_target: str):
    shell_command = f"rm -rf {rm_target}"
    exit_val = os.system(shell_command)
    logger.info(f"Command '{shell_command}' returned with exit value: {exit_val}")
    if exit_val != 0:
        raise Exception("Shell command failed")


def write_to_csv(ddf: dd.DataFrame, include_index: bool = True) -> FilePaths:
    target_dir = '/tmp/expunge_data'
    target_glob = f"{target_dir}/expunge-*.csv"
    logger.info(f"Writing data to: {target_dir}")

    logger.info("Clearing any data from previous runs")
    rm_cmd(target_glob)

    logger.info("Executing Dask task graph and writing results to CSV...")
    file_paths = ddf.to_csv(target_glob, index=include_index)
    logger.info("File(s) written successfully")

    return file_paths


def copy_files_to_db(
    table: sa.Table, 
    file_paths: FilePaths,
    engine: Engine
):
    columns = extract_table_columns(table, exclude_autoincrement=True)
    
    # Extracting the underlying Psycopg2 connection to access
    # bulk loading features not exposed by SQLAlchemy
    db_conn = engine.raw_connection()

    with db_conn.cursor() as cursor:
        for path in file_paths:
            logger.info(f"Loading from file: {path}")
            with open(path, 'r') as file:
                cursor.copy_expert(f"""
                    COPY {table.name} (
                        {','.join(columns)}
                    )
                    FROM STDIN
                    WITH CSV HEADER
                """, file)
                
    db_conn.commit()
    logger.info(f"Files loaded to table: '{table.name}'")


def load_to_db(
    ddf: dd.DataFrame, 
    target_table: sa.Table,
    engine: Engine, 
    include_index: bool = True
):
    logger.info(f"Beginning bulk load to table: '{target_table.name}'")
    file_paths = write_to_csv(ddf, include_index=include_index)
    copy_files_to_db(target_table, file_paths, engine)
