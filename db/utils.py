import logging
from typing import Union, List
import os

import psycopg2
import sqlalchemy as sa
import dask.dataframe as dd
import pandas as pd

from db.models import Charges, Features, Base
from expunge.config_parser import ExpungeConfig


logger = logging.getLogger(__name__)

USER = 'jupyter'
PASSWORD = os.environ['POSTGRES_PASS']
HOST = 'localhost'
PORT = '5432'
DB = 'expunge'

DATABASE_URI = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"


def get_db_engine():
    return sa.create_engine(DATABASE_URI)


def get_psycopg2_conn():
    return psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DB
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


def write_to_csv(ddf: dd.DataFrame) -> List[str]:
    target_dir = '/tmp/expunge_data'
    target_glob = f"{target_dir}/expunge_features-*.csv"
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


def load_to_db(file_paths: List[str], config: ExpungeConfig):
    logger.info("Opening connection to PostGres via Psycopg")
    conn = get_psycopg2_conn()

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


# Running this script directly builds the database
# tables in PostGres
if __name__ == '__main__':
    engine = sa.create_engine(DATABASE_URI, echo=True)
    Base.metadata.create_all(engine)