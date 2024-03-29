import logging
from typing import Tuple
from functools import lru_cache

import numpy as np
import pandas as pd
from sklearn import tree
from sklearn.preprocessing import OneHotEncoder
import dask.dataframe as dd

from db.dask_utils import extract_table_columns
from db.models import features, outcomes
from expunge.config_parser import RunConfig


logger = logging.getLogger(__name__)

DEFAULT_TRAINING_SET_LOCATION = './data/training_set.csv'


def load_training_set(training_set_path: str = DEFAULT_TRAINING_SET_LOCATION) -> pd.DataFrame:
    logger.info(f"Loading decision tree training set from: {training_set_path}")
    return pd.read_csv(training_set_path)


def split_training_set(training_set: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    X = training_set.drop('expungability', axis='columns')
    Y = training_set['expungability']
    return X, Y


def train_encoder(X: pd.DataFrame) -> OneHotEncoder:
    logger.info("Training one-hot encoder using training data")
    encoder = OneHotEncoder()
    return encoder.fit(X)


def train_decision_tree(X: pd.DataFrame, Y: pd.DataFrame) -> tree.DecisionTreeClassifier:
    logger.info("Training expungement classification decision tree")
    decision_tree = tree.DecisionTreeClassifier()
    return decision_tree.fit(X, Y)


@lru_cache(maxsize=None) # Cache pre-built classifier, preventing re-training
def build_encoder_and_classifier() -> Tuple[OneHotEncoder, tree.DecisionTreeClassifier]:
    training_set = load_training_set()
    X, Y = split_training_set(training_set)
    
    encoder = train_encoder(X)

    X = encoder.transform(X)
    decision_tree = train_decision_tree(X, Y)

    return encoder, decision_tree


def classify_frame(features: pd.DataFrame) -> pd.Series:
    encoder, classifier = build_encoder_and_classifier()

    needed_features = features[encoder.feature_names_in_]
    encoded_features = encoder.transform(needed_features)

    return classifier.predict(encoded_features)


def get_sameday_disqualifier(df: pd.DataFrame) -> pd.Series:
    df = df.copy()
    df['is_not_auto'] = ~df['expungability'].isin(['Automatic', 'Automatic (pending)'])
    return df.groupby(['person_id', 'hearing_date'])['is_not_auto'].transform('any')


def apply_sameday_rule(ddf: dd.DataFrame) -> dd.DataFrame:
    ddf['sameday_disqualifier'] = ddf.map_partitions(
        get_sameday_disqualifier,
        meta=pd.Series(dtype=bool)
    )

    ddf['expungability'] = ddf['expungability'].mask((
        (ddf['sameday_disqualifier']==True)
        & (ddf['expungability']=='Automatic')
    ), 'Petition')

    ddf['expungability'] = ddf['expungability'].mask((
        (ddf['sameday_disqualifier']==True)
        & (ddf['expungability']=='Automatic (pending)')
    ), 'Petition (pending)')

    return ddf


def get_lifetime_disqualifier(df: pd.DataFrame) -> pd.Series:
    df = df.copy()
    
    df['is_eligible'] = df['expungability'] != 'Not eligible'
    df['running_expunge_count'] = df.groupby('person_id')['is_eligible'].cumsum()
    df['over_2_expungements'] = df['running_expunge_count'] > 2
    df['lifetime_is_applicable'] = (
        (df['disposition_category']=='Conviction') 
        | (
            (df['disposition_category']=='Deferral Dismissal') 
            & (df['sameday_disqualifier'])
        )
    )

    return (df['over_2_expungements']) & (df['lifetime_is_applicable'])


def apply_lifetime_rule(ddf: dd.DataFrame) -> dd.DataFrame:
    ddf['lifetime_disqualifier'] = ddf.map_partitions(
        get_lifetime_disqualifier,
        meta=pd.Series(dtype=bool)
    )
    ddf['expungability'] = ddf['expungability'].mask(
        ddf['lifetime_disqualifier']==True, 
        'Not eligible'
    )
    return ddf


def split_tables(ddf: dd.DataFrame) -> Tuple[dd.DataFrame, dd.DataFrame]:
    ddf = ddf.rename(columns={'id': 'charge_id'})
    features_cols = extract_table_columns(features, exclude_autoincrement=True)
    outcomes_cols = extract_table_columns(outcomes, exclude_autoincrement=True)
    return ddf[features_cols], ddf[outcomes_cols]


def classify_distributed_frame(
    ddf: dd.DataFrame, 
    config: RunConfig
) -> Tuple[dd.DataFrame, dd.DataFrame]:
    ddf['expungability'] = ddf.map_partitions(
        classify_frame,
        meta=pd.Series(dtype=str)
    )

    if config.sameday_rule:
        ddf = apply_sameday_rule(ddf)
    else:
        ddf['sameday_disqualifier'] = np.NaN
        
    if config.lifetime_rule:
        ddf = apply_lifetime_rule(ddf)
    else:
        ddf['lifetime_disqualifier'] = np.NaN
    
    logger.info("Executing all queued Dask tasks...")
    ddf = ddf.persist()

    return split_tables(ddf)
