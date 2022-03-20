import logging
from datetime import datetime

import dask.dataframe as dd
import sqlalchemy as sa
from sqlalchemy.orm import Session

from expunge.config_parser import RunConfig
from db.models import charges, Runs
from db.dask_utils import ddf_from_table


logger = logging.getLogger(__name__)


def clear_previous_run_by_id(run_id: str, session: Session):
    results = session.execute(
        sa.select(Runs.id)
    )
    run_ids = [id for id in results.scalars()]
    if run_id in run_ids:
        logger.info(f"Run ID '{run_id}' already exists")
        logger.info("Removing data from previous run")
        session.execute(
            sa.delete(Runs)
              .where(Runs.id == run_id)
        )


def initialize_run(config: RunConfig, session: Session) -> RunConfig:
    """Create a row for the expungement classification run in `Runs` table"""
    clear_previous_run_by_id(config.id, session)

    config.start_at = datetime.now()
    config.status = 'In Progress'

    logger.info(f"Loading run metadata to 'runs' table")
    session.add(config)
    session.commit()

    return config


def finish_run(
    config: RunConfig, 
    session: Session
) -> RunConfig:
    config.end_at = datetime.now()

    logger.info(f"Updating run status to: '{config.status}'")
    session.flush()
    session.commit()


def fetch_charges(config: RunConfig, npartitions: int = None) -> dd.DataFrame:
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
