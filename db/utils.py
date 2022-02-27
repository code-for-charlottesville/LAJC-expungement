import logging
import os
from typing import List

import sqlalchemy as sa

from db.models import Base as BaseModel


logger = logging.getLogger(__name__)

USER = 'jupyter'
PASSWORD = os.environ['POSTGRES_PASS']
HOST = 'localhost'
PORT = '5432'
DB = 'expunge'

DATABASE_URI = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"


def extract_model_columns(
    model: BaseModel,
    exclude_autoincrement: bool = False
) -> List[str]:
    table: sa.Table = model.__table__
    columns = [
        col.name for col in table.columns
        if not (col.autoincrement == True and exclude_autoincrement)
    ]
    return columns


def create_db_engine(echo: bool = False) -> sa.engine.Engine:
    logger.info("Opening connection to PostGres via SQLAlchemy")
    return sa.create_engine(DATABASE_URI, echo=echo)
