"""Running this script directly builds the database tables in PostGres"""

from db.utils import create_db_engine
from db.models import Base


if __name__ == '__main__':
    engine = create_db_engine(echo=True)
    Base.metadata.create_all(engine)
