import logging
from datetime import date

from sqlalchemy.engine import Engine
from distributed import Client as DaskClient
import dask.dataframe as dd

from db.utils import DATABASE_URI, create_db_engine
from db.dask_utils import load_to_db
from db.models import charges, runs, features, outcomes


logger = logging.getLogger(__name__)


def read_raw_data(npartitions: int) -> dd.DataFrame:
    logger.info('Reading raw data into Dask')
    return dd.read_sql_table(
        'expunge',
        DATABASE_URI,
        index_col='id',
        npartitions=npartitions
    )


def rename_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    logger.info("Queueing column renaming")
    column_map = {
        'person_id': 'person_id',
        'HearingDate': 'hearing_date',
        'CodeSection': 'code_section',
        'ChargeType': 'charge_type',
        'Class': 'charge_class',
        'DispositionCode': 'disposition_code',
        'Plea': 'plea',
        'Race': 'race',
        'Sex': 'sex',
        'fips': 'fips'
    }
    needed_cols = list(column_map.values())
    return ddf.rename(columns=column_map)[needed_cols]


def filter_hearing_dates(ddf: dd.DataFrame) -> dd.DataFrame:
    logger.info("Queueing date filtering")
    # TODO: Investigate valid reasons for future hearing dates. 
    ddf = ddf[
        ddf['hearing_date'].apply(
            lambda x: x <= date.today(), 
            meta=('hearing_date', 'object')
        )
    ]
    return ddf


def standardize_race(ddf: dd.DataFrame) -> dd.DataFrame:
    logger.info("Queueing race column standardization")
    race: dd.Series = ddf['race'].str.upper()
    ddf['race'] = (
        ddf['race']
            .mask(race.str.contains('BLACK'), 'Black')
            .mask(
                race.str.contains('WHITE')
                | race.str.contains('CAUCASIAN'),
                'White'
            )
            .mask(race.str.contains(
                # https://regex101.com/r/nJazne/1
                r"(?<![A-Z])ASIAN", regex=True),
                'Asian or Pacific Islander'
            )
            .mask(
                race.str.contains('AMERICAN INDIAN')
                | race.str.contains('NATIVE'),
                'American Indian or Alaskan Native'
            )
            .mask(
                race.str.contains('OTHER')
                | race.str.contains('MISSING')
                | race.str.contains('UNKNOWN'),
                'Unknown'
            )
    )
    return ddf


def fix_fips_codes(ddf: dd.DataFrame) -> dd.DataFrame:
    logger.info("Queueing fips code fixes")
    ddf['fips'] = (ddf['fips'].astype(str)
                              .str.pad(3, fillchar='0'))
    return ddf


def clean_data(ddf: dd.DataFrame) -> dd.DataFrame:
    logger.info("Building task graph to clean data")
    return (
        ddf.pipe(rename_columns)
           .pipe(filter_hearing_dates)
           .pipe(standardize_race)
           .pipe(fix_fips_codes)
    )


def load_charges(
    engine: Engine, 
    npartitions: int = None,
    clear_existing: bool = False
):
    ddf = read_raw_data(npartitions)
    ddf = clean_data(ddf)
    
    if clear_existing:
        logger.info("Clearing any existing expungement data")
        for table in [runs, charges, features, outcomes]:
            logger.info(f"Deleting from: {table.name}")
            engine.execute(f"""
                DELETE FROM {table.name}
            """)
        
    load_to_db(
        ddf, 
        target_table=charges, 
        engine=engine,
        include_index=False
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s - %(module)s.py]: %(message)s',
        datefmt='%H:%M:%S'
    )
    DaskClient()
    engine = create_db_engine()
    load_charges(engine, clear_existing=True)
