import logging
from dataclasses import dataclass
from typing import List

import yaml


logger = logging.getLogger(__name__)


@dataclass
class ExpungeConfig:
    covered_sections_a: List[str]
    covered_sections_b: List[str]
    covered_sections_b_misdemeanor: List[str]
    excluded_sections_twelve: List[str]

    years_passed_disqualifier_short: int
    years_passed_disqualifier_long: int
    years_since_arrest_disqualifier: int
    years_since_felony_disqualifier: int

    lifetime_rule: bool
    sameday_rule: bool

    @classmethod
    def from_yaml(cls, file_path: str):
        logger.info(f"Loading config from file: {file_path}")
        with open(file_path) as file:
            yaml_dict = yaml.safe_load(file)
        return cls(**yaml_dict)
