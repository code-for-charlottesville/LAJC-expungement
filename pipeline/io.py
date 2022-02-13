import logging
from typing import Union, List
import os

import sqlalchemy as sa
import dask.dataframe as dd
import pandas as pd

from pipeline.config import ExpungeConfig
from pipeline.db import (
    DATABASE_URI, 
    expunge, 
    expunge_features,
    get_psycopg2_conn
)


logger = logging.getLogger(__name__)


def fetch_expunge_data(
    config: ExpungeConfig,
    n_partitions: Union[int, None] = None,
    custom_query: Union[sa.sql.Select, None] = None
) -> dd.DataFrame:
    """Fetches expungement records and loads them into a Dask DataFrame. 

    Args:
        n_partitions: The number of underlying Pandas DataFrames to partition
            the table into (partitioned by 'person_id'). If None, we allow 
            Dask to choose automatically. 
        custom_query: A custom SQLAlchemy query object that will be used to
            query data. If not passed, Dask will fetch all data from 
            the expunge_model table, sorted by person_id and HearingDate. 
    """
    query = custom_query if custom_query is not None else (
        sa.select(expunge)
            .where(
                # Filter out any records with future hearing dates
                expunge.c.HearingDate < config.cutoff_date
            )
            .order_by(
                expunge.c.person_id,
                expunge.c.HearingDate
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

    logger.info(f"Reading from table: {expunge.name}")
    if n_partitions:
        logger.info(f"Loading into {n_partitions} partitions")

    return dd.read_sql_table(
        table=query,
        index_col='person_id',
        uri=DATABASE_URI,
        meta=meta_frame,
        **kwargs
    )


def write_to_csv(ddf: dd.DataFrame) -> List[str]:
    target_dir = '/tmp/expunge_data'
    target_glob = f"{target_dir}/expunge_features-*.csv"
    logger.info(f"Expungement feature data will be written to: {target_dir}")

    logger.info("Clearing any data from previous runs")
    shell_command = f"rm -rf {target_glob}"
    exit_val = os.system(f'rm -rf {target_glob}')
    logger.info(f"Command '{shell_command}' returned with exit value: {exit_val}")

    # Reorder columns to match DB table
    ddf = ddf[[col for col in expunge_features.columns.keys() if col != 'person_id']]

    logger.info("Executing Dask task graph and writing results to CSV...")
    file_paths = ddf.to_csv(target_glob)
    logger.info("File(s) written successfully")

    return file_paths


def load_to_db(file_paths: List[str], config: ExpungeConfig):
    logger.info("Opening connection to PostGres via Psycopg")
    conn = get_psycopg2_conn()

    with conn:
        with conn.cursor() as cursor:
            logger.info(f"Deleting any records with run_id: {config.run_id}")
            cursor.execute(f"""
                DELETE FROM {expunge_features.name}
                WHERE run_id = '{config.run_id}'
            """)
            for path in file_paths:
                logger.info(f"Loading from file: {path}")
                with open(path, 'r') as file:
                    cursor.copy_expert(f"""
                        COPY {expunge_features.name}
                        FROM STDIN
                        WITH CSV HEADER
                    """, file)

    logger.info(f"Load to DB complete")
