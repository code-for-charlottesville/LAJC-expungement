import logging

import dask.dataframe as dd
import sqlalchemy as sa

from expunge.config_parser import ExpungeConfig
from db.models import Charges
from db.dask_utils import ddf_from_model


logger = logging.getLogger(__name__)


def fetch_charges(config: ExpungeConfig, npartitions: int = None) -> dd.DataFrame:
    query = (
        sa.select(Charges)
            .where(
                Charges.hearing_date < config.cutoff_date
            )
            .order_by(
                Charges.person_id,
                Charges.hearing_date
            )
    )
    logger.info(f"Reading from table: {Charges.__tablename__}")
    return ddf_from_model(
        model=Charges,
        index_col='person_id',
        custom_query=query,
        npartitions=npartitions
    )
