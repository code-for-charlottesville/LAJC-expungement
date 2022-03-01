import logging
import argparse

from distributed import Client as DaskClient

from db.dask_utils import (
    write_to_csv, 
    load_to_db
)
from db.models import Features
from expunge.config_parser import ExpungeConfig
from expunge.featurize import build_features
from expunge.classify import classify_in_parallel


logger = logging.getLogger(__name__)


def run_classification(config: ExpungeConfig, n_partitions: int = None) -> str:
    logger.info(f"Classification Run ID: {config.run_id}")
    
    logger.info("Initializing Dask distributed client")
    DaskClient()
    
    ddf = fetch_expunge_data(config, n_partitions)

    ddf = build_features(ddf, config)
    ddf = classify_in_parallel(ddf, config)

    file_paths = write_to_csv(ddf)
    load_to_db(file_paths, config)

    logger.info(f"Expungement classification complete!")
    logger.info(f"""
        Query results with: 

        SELECT * 
        FROM {Features.__tablename__} 
        WHERE run_id = '{config.run_id}'
    """)

    return config.run_id


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
        '-c', '--config',
        type=str,
        help='Path to expungement configuration file',
        default='configs/default.yaml'
    )
    args = parser.parse_args()

    DaskClient()
    config = ExpungeConfig.from_yaml(args.config)

    run_classification(config, args.partitions)
