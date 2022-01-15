import os

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Text,
    Integer,
    Date,
    BigInteger,
    Boolean,
    Float,
    create_engine
)
import psycopg2


USER = 'jupyter'
PASSWORD = os.environ['POSTGRES_PASS']
HOST = 'localhost'
PORT = '5432'
DB = 'expunge'

DATABASE_URI = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

meta = MetaData()

EXPUNGE_TABLE = 'expunge_clean' # Full Dataset
# EXPUNGE_TABLE = 'expunge_10k_clean' # ~26K records
# EXPUNGE_TABLE = 'expunge_1k_clean' # ~2.6K records

FEATURE_TABLE = 'expunge_features'

expunge = Table(EXPUNGE_TABLE, meta,
    # TODO: Add a unique record ID to the expungement data. 
    # This will likely be helpful in QC and comparisons. 

    # Column('record_id', BigInteger, primary_key=True),

    Column('person_id', BigInteger),
    Column('HearingDate', Date),
    Column('CodeSection', Text),
    Column('ChargeType', Text),
    Column('Class', Text),
    Column('DispositionCode', Text),
    Column('Plea', Text),
    Column('Race', Text),
    Column('Sex', Text),
    Column('fips', Integer),
)

expunge_features = Table(FEATURE_TABLE, meta,
    # Column('record_id', BigInteger, primary_key=True),

    Column('person_id', BigInteger),
    Column('HearingDate', Date),
    Column('CodeSection', Text),
    Column('ChargeType', Text),
    Column('Class', Text),
    Column('DispositionCode', Text),
    Column('Plea', Text),
    Column('Race', Text),
    Column('Sex', Text),
    Column('fips', Integer),
    
    Column('disposition', Text),
    Column('chargetype', Text),
    Column('codesection', Text),
    Column('convictions', Boolean),

    Column('last_hearing_date', Date),
    Column('last_felony_conviction_date', Date),
    Column('next_conviction_date', Date),

    Column('last_hearing_delta', Float),
    Column('last_felony_conviction_delta', Float),
    Column('next_conviction_delta', Float),
    Column('from_present_delta', Float),

    Column('arrest_disqualifier', Boolean),
    Column('felony_conviction_disqualifier', Boolean),
    Column('next_conviction_disqualifier_after_misdemeanor', Boolean),
    Column('next_conviction_disqualifier_after_felony', Boolean),
    Column('pending_after_misdemeanor', Boolean),
    Column('pending_after_felony', Boolean),
    Column('class1_2', Boolean),
    Column('class3_4', Boolean),

    Column('run_id', Text)
)


def get_psycopg2_conn():
    return psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DB
    )


if __name__ == '__main__':
    engine = create_engine(DATABASE_URI, echo=True)
    meta.create_all(engine)
