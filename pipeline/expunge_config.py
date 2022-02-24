import logging
from dataclasses import dataclass
from typing import List

import yaml
import numpy as np


logger = logging.getLogger(__name__)


@dataclass
class ExpungeConfig:
    run_id: str

    cutoff_date: str

    covered_sections_a: List[str]
    covered_sections_b: List[str]
    covered_sections_b_misdemeanor: List[str]
    excluded_sections_twelve: List[str]

    years_since_arrest: np.timedelta64
    years_since_felony: np.timedelta64
    years_until_conviction_after_misdemeanor: np.timedelta64
    years_until_conviction_after_felony: np.timedelta64

    lifetime_rule: bool
    sameday_rule: bool

    @staticmethod
    def cast_timedeltas(attrs: dict) -> dict:
        """Convert integer configs to timedeltas (years)"""
        timedelta_vars = [
            'years_since_arrest',
            'years_since_felony',
            'years_until_conviction_after_misdemeanor',
            'years_until_conviction_after_felony',
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
