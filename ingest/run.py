import logging

from distributed import Client as DaskClient

from db.utils import create_db_engine
from ingest.charges import load_charges


logger = logging.getLogger(__name__)


def run_ingestion(clear_existing: bool = True):
    logger.info("Starting expungement data ingestion pipeline")
    DaskClient()
    engine = create_db_engine()
    load_charges(engine, clear_existing=clear_existing)
