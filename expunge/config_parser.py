import logging

import yaml

from db.models import Runs


logger = logging.getLogger(__name__)

RunConfig = Runs


def parse_config_file(file_path: str) -> RunConfig:
    logger.info(f"Loading config from file: {file_path}")
    with open(file_path) as file:
        config_yaml: dict = yaml.safe_load(file)
    config_yaml['id'] = config_yaml.pop('run_id')
    return Runs(**config_yaml)
