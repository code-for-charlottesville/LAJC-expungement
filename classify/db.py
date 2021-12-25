import os

import sqlalchemy as sa


_USER = 'jupyter'
_PASSWORD = os.environ['POSTGRES_PASS']
_HOST = 'localhost'
_PORT = '5432'
_DB = 'expunge'

DATABASE_URI = f"postgresql://{_USER}:{_PASSWORD}@{_HOST}:{_PORT}/{_DB}"


_EXPUNGE_TABLE = 'expunge_clean' # Full Dataset
# _EXPUNGE_TABLE = 'expunge_10k_clean' # ~26K records
# _EXPUNGE_TABLE = 'expunge_1k_clean' # ~2.6K records

expunge_model = sa.Table(_EXPUNGE_TABLE, sa.MetaData(),
    sa.Column('person_id', sa.Integer),
    sa.Column('HearingDate', sa.DateTime),
    sa.Column('CodeSection', sa.String),
    sa.Column('ChargeType', sa.String),
    sa.Column('Class', sa.String),
    sa.Column('DispositionCode', sa.String),
    sa.Column('Plea', sa.String),
    sa.Column('Race', sa.String),
    sa.Column('Sex', sa.String),
    sa.Column('fips', sa.Integer),
)
