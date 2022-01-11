import logging
from dataclasses import dataclass
from typing import List

import yaml
import numpy as np


logger = logging.getLogger(__name__)


@dataclass
class ExpungeConfig:
    covered_sections_a: List[str]
    covered_sections_b: List[str]
    covered_sections_b_misdemeanor: List[str]
    excluded_sections_twelve: List[str]

    years_passed_disqualifier_short: np.timedelta64
    years_passed_disqualifier_long: np.timedelta64
    years_since_arrest_disqualifier: np.timedelta64
    years_since_felony_disqualifier: np.timedelta64
    years_until_next_conviction_short: np.timedelta64
    years_until_next_conviction_long: np.timedelta64

    lifetime_rule: bool
    sameday_rule: bool

    @staticmethod
    def cast_timedeltas(attrs: dict) -> dict:
        """Convert integer configs to timedeltas (years)"""
        timedelta_vars = [
            'years_passed_disqualifier_short',
            'years_passed_disqualifier_long',
            'years_since_arrest_disqualifier',
            'years_since_felony_disqualifier',
            'years_until_next_conviction_short',
            'years_until_next_conviction_long'
        ]
        for var in timedelta_vars:
            attrs[var] = np.timedelta64(attrs[var], 'Y')
        return attrs

    @classmethod
    def from_yaml(cls, file_path: str):
        logger.info(f"Loading config from file: {file_path}")
        with open(file_path) as file:
            yaml_dict = yaml.safe_load(file)
        yaml_dict = cls.cast_timedeltas(yaml_dict)
        return cls(**yaml_dict)
