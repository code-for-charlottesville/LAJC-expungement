import logging
import datetime
from typing import Union, List, Any
import os

import sqlalchemy as sa
from sqlalchemy.engine import Engine
import dask.dataframe as dd
import pandas as pd

from db.utils import (
    DATABASE_URI, 
    create_db_engine, 
    extract_model_columns
)
from db.models import Charges, Features, Base as BaseModel
from expunge.config_parser import ExpungeConfig


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
    model: BaseModel, 
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
    table: sa.Table = model.__table__
    meta_dict = {
        c.name: python_dt_type_to_numpy(c.type.python_type)
        for c in table.columns
    }
    df: pd.DataFrame = dd.utils.make_meta(meta_dict)

    if index_col:
        df = df.drop(index_col, axis='columns')
    
    return df


def ddf_from_model(
    model: BaseModel,
    index_col: str,
    custom_query: sa.sql.Selectable = None,
    npartitions: int = None
) -> dd.DataFrame:
    query = sa.select(model) if custom_query is None else custom_query
    types_meta = extract_dask_meta(model, index_col=index_col)
    return dd.read_sql_query(
        sql=query,
        con=DATABASE_URI,
        index_col=index_col,
        meta=types_meta,
        npartitions=npartitions
    )


def fetch_expunge_data(
    config: ExpungeConfig,
    n_partitions: Union[int, None] = None,
    custom_query: Union[sa.sql.Selectable, None] = None
) -> dd.DataFrame:
    """Fetches criminal records and loads them into a Dask DataFrame. 

    Args:
        n_partitions: The number of underlying Pandas DataFrames to partition
            the table into (partitioned by 'person_id'). If None, we allow 
            Dask to choose automatically. 
        custom_query: A custom SQLAlchemy query object that will be used to
            query data. If not passed, Dask will fetch all data from 
            the charges table, sorted by person_id and HearingDate. 
    """
    query = custom_query if custom_query is not None else (
        sa.select(Charges)
            .where(
                # Filter out any records with future hearing dates
                Charges.hearing_date < config.cutoff_date
            )
            .order_by(
                Charges.person_id,
                Charges.hearing_date
            )
    )

    dask_types = {
        # 'record_id': 'int64',
        'HearingDate': 'datetime64[ns]',
        'CodeSection': str,
        'ChargeType': str,
        'Class': str,
        'DispositionCode': str,
        'Plea': str,
        'Race': str,
        'Sex': str,
        'fips': 'int64'
    }
    meta_frame = pd.DataFrame(columns=dask_types.keys()).astype(dask_types)

    kwargs = {'npartitions': n_partitions} if n_partitions else {}

    logger.info(f"Reading from table: {Charges.__tablename__}")
    if n_partitions:
        logger.info(f"Loading into {n_partitions} partitions")

    return dd.read_sql_table(
        table=query,
        index_col='person_id',
        uri=DATABASE_URI,
        meta=meta_frame,
        **kwargs
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


def write_features_to_csv(ddf: dd.DataFrame) -> List[str]:
    target_dir = '/tmp/expunge_data'
    target_glob = f"{target_dir}/expunge-*.csv"
    logger.info(f"Expungement feature data will be written to: {target_dir}")

    logger.info("Clearing any data from previous runs")
    shell_command = f"rm -rf {target_glob}"
    exit_val = os.system(f'rm -rf {target_glob}')
    logger.info(f"Command '{shell_command}' returned with exit value: {exit_val}")

    # Reorder columns to match DB table
    column_names = Features.__table__.columns.keys()
    ddf = ddf[[col for col in column_names if col != 'person_id']]

    logger.info("Executing Dask task graph and writing results to CSV...")
    file_paths = ddf.to_csv(target_glob)
    logger.info("File(s) written successfully")

    return file_paths


def copy_files_to_db(
    model: BaseModel, 
    file_paths: FilePaths,
    engine: Engine
):
    columns = extract_model_columns(model, exclude_autoincrement=True)
    
    # Extracting the underlying Psycopg2 connection to access
    # bulk loading features not exposed by SQLAlchemy
    db_conn = engine.raw_connection()

    with db_conn.cursor() as cursor:
        for path in file_paths:
            logger.info(f"Loading from file: {path}")
            with open(path, 'r') as file:
                cursor.copy_expert(f"""
                    COPY {model.__tablename__} (
                        {','.join(columns)}
                    )
                    FROM STDIN
                    WITH CSV HEADER
                """, file)
                
    db_conn.commit()
    logger.info(f"Files loaded to table: '{model.__tablename__}'")


def copy_results_to_db(file_paths: List[str], config: ExpungeConfig):
    engine = create_db_engine()
    conn = engine.raw_connection()

    with conn:
        with conn.cursor() as cursor:
            logger.info(f"Deleting any records with run_id: {config.run_id}")
            cursor.execute(f"""
                DELETE FROM {Features.__tablename__}
                WHERE run_id = '{config.run_id}'
            """)
            for path in file_paths:
                logger.info(f"Loading from file: {path}")
                with open(path, 'r') as file:
                    cursor.copy_expert(f"""
                        COPY {Features.__tablename__}
                        FROM STDIN
                        WITH CSV HEADER
                    """, file)

    logger.info(f"Load to DB complete")


def load_to_db(
    ddf: dd.DataFrame, 
    target_model: BaseModel,
    engine: Engine, 
    include_index: bool = True
):
    file_paths = write_to_csv(ddf, include_index=include_index)
    copy_files_to_db(target_model, file_paths, engine)
