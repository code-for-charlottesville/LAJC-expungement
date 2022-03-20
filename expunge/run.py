import logging
from typing import Optional

from distributed import Client as DaskClient

from db.utils import create_db_session
from db.dask_utils import load_to_db
from db.models import features, outcomes
from expunge.data import (
    initialize_run, 
    fetch_charges,
    finish_run
)
from expunge.config_parser import parse_config_file
from expunge.featurize import build_features
from expunge.classify import classify_distributed_frame


logger = logging.getLogger(__name__)


def run_classification(
    config_path: str, 
    write_features: bool = False,
    n_partitions: Optional[int] = None
) -> str:
    config = parse_config_file(config_path)
    logger.info(f"Classification Run ID: {config.id}")

    session = create_db_session()
    config = initialize_run(config, session)
    
    try:
        logger.info("Initializing Dask distributed client")
        DaskClient()
        
        ddf = fetch_charges(config, n_partitions)

        ddf = build_features(ddf, config)
        features_ddf, outcomes_ddf = classify_distributed_frame(ddf, config)

        if write_features:
            load_to_db(
                features_ddf,
                target_table=features,
                engine=session.bind,
                include_index=False
            )

        load_to_db(
            outcomes_ddf,
            target_table=outcomes,
            engine=session.bind,
            include_index=False
        )

        logger.info(f"Expungement classification complete!")
        config.status = 'Completed'
    except KeyboardInterrupt:
        logger.info(f"Canceling classification run")
        config.status = 'Canceled'
    except Exception as e:
        logger.info(e)
        logger.info("Exiting classification run")
        config.status = 'Failed'
    finally:
        run_id = config.id
        finish_run(config, session)
        session.close()

    return run_id
