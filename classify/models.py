# %%
from sqlalchemy import create_engine, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import (
    BigInteger,
    Integer, 
    Text,
    DateTime
)

from classify.db import DATABASE_URI


Base = declarative_base()


class Expunge(Base):
    """Main Expungement Data Table"""
    __tablename__ = 'expunge_clean'

    record_id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    HearingDate = Column(DateTime)
    CodeSection = Column(Text)
    ChargeType = Column(Text)
    Class = Column(Text)
    DispositionCode = Column(Text)
    Plea = Column(Text)
    Race = Column(Text)
    Sex = Column(Text)
    fips = Column(Integer)


class ExpungeFeatures(Base):
    """Expungement Classification Features Table"""
    __tablename__ = 'expunge_features'

    record_id = Column(BigInteger, primary_key=True)
    person_id = Column(BigInteger)
    HearingDate = Column(DateTime)
    CodeSection = Column(Text)
    ChargeType = Column(Text)
    Class = Column(Text)
    DispositionCode = Column(Text)
    Plea = Column(Text)
    Race = Column(Text)
    Sex = Column(Text)
    fips = Column(Integer)

    


# %%
if __name__ == '__main__':
    engine = create_engine(DATABASE_URI, echo=True)
    Base.metadata.create_all(engine)
