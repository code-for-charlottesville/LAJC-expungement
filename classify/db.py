import os

from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Text,
    Integer,
    Date,
    BigInteger,
    create_engine
)


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

expunge = Table(EXPUNGE_TABLE, meta,
    Column('record_id', BigInteger, primary_key=True),

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

expunge_features = Table('expunge_features', meta,
    Column('record_id', BigInteger, primary_key=True),

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
    Column('convictions', Integer),

    Column('last_hearing_date', Date),
    Column('last_felony_conviction_date', Date),
    Column('next_conviction_date', Date),

    Column('last_hearing_delta', Integer),
    Column('last_felony_conviction_delta', Integer),
    Column('next_conviction_delta', Integer),
    Column('from_present_delta', Integer),

    Column('arrest_disqualifier', Integer),
    Column('felony_conviction_disqualifier', Integer),
    Column('next_conviction_disqualifier_short', Integer),
    Column('next_conviction_disqualifier_long', Integer),
    Column('from_present_disqualifier_short', Integer),
    Column('from_present_disqualifier_long', Integer),
    Column('class1_2', Integer),
    Column('class3_4', Integer),
)

if __name__ == '__main__':
    engine = create_engine(DATABASE_URI, echo=True)
    meta.create_all(engine)
