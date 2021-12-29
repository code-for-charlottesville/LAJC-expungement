import os

import sqlalchemy as sa


USER = 'jupyter'
PASSWORD = os.environ['POSTGRES_PASS']
HOST = 'localhost'
PORT = '5432'
DB = 'expunge'

DATABASE_URI = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"


EXPUNGE_TABLE = 'expunge_clean' # Full Dataset
# EXPUNGE_TABLE = 'expunge_10k_clean' # ~26K records
# EXPUNGE_TABLE = 'expunge_1k_clean' # ~2.6K records



expunge_model = sa.Table(EXPUNGE_TABLE, sa.MetaData(),
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
