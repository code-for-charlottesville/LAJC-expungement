import logging

import dask.dataframe as dd
import sqlalchemy as sa

from expunge.config_parser import ExpungeConfig
from db.models import charges
from db.dask_utils import ddf_from_table


logger = logging.getLogger(__name__)


def fetch_charges(config: ExpungeConfig, npartitions: int = None) -> dd.DataFrame:
    query = (
        sa.select(charges)
            .where(
                charges.c.hearing_date < config.cutoff_date
            )
            .order_by(
                charges.c.person_id,
                charges.c.hearing_date
            )
    )
    logger.info(f"Reading from table: {charges.name}")
    return ddf_from_table(
        table=charges,
        index_col='person_id',
        custom_query=query,
        npartitions=npartitions
    )
