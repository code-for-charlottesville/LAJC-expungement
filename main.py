import logging
import argparse

from distributed import Client as DaskClient

from db.utils import create_db_session
from db.dask_utils import load_to_db
from db.models import features, outcomes
from expunge.data import (
    initialize_run, 
    fetch_charges,
    finish_run
)
from expunge.config_parser import RunConfig, parse_config_file
from expunge.featurize import build_features
from expunge.classify import run_classification


logger = logging.getLogger(__name__)


def classify(
    config: RunConfig, 
    write_features: bool = False,
    n_partitions: int = None
) -> str:
    logger.info(f"Classification Run ID: {config.id}")
    session = create_db_session()
    config = initialize_run(config, session)
    
    try:
        logger.info("Initializing Dask distributed client")
        DaskClient()
        
        ddf = fetch_charges(config, n_partitions)

        ddf = build_features(ddf, config)
        features_ddf, outcomes_ddf = run_classification(ddf, config)

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


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s - %(module)s.py]: %(message)s',
        datefmt='%H:%M:%S'
    )

    parser = argparse.ArgumentParser(
        description='Build expungement classification features'
    )
    parser.add_argument(
        '-p', '--partitions',
        type=int,
        help='Number of partitions (Pandas DFs) to split data into',
        default=None
    )
    parser.add_argument(
        '-f', '--write-features',
        action='store_true',
        help='Flag indicating to save classification features to DB',
    )
    parser.add_argument(
        '-c', '--config',
        type=str,
        help='Path to expungement configuration file',
        default='expunge/configs/default.yaml'
    )
    args = parser.parse_args()

    config = parse_config_file(args.config)

    classify(config, args.write_features, args.partitions)
