import logging
from typing import Tuple
from functools import lru_cache

import pandas as pd
from sklearn import tree
from sklearn.preprocessing import OneHotEncoder
import dask.dataframe as dd

from pipeline.config import ExpungeConfig


logger = logging.getLogger(__name__)

DEFAULT_TRAINING_SET_LOCATION = 'pipeline/training_set.csv'


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


@lru_cache(maxsize=None)
def build_encoder_and_classifier() -> Tuple[OneHotEncoder, tree.DecisionTreeClassifier]:
    training_set = load_training_set()
    X, Y = split_training_set(training_set)
    
    encoder = train_encoder(X)

    X = encoder.transform(X)
    decision_tree = train_decision_tree(X, Y)

    return encoder, decision_tree


def classify_features(features: pd.DataFrame) -> pd.Series:
    encoder, classifier = build_encoder_and_classifier()

    needed_features = features[encoder.feature_names_in_]
    encoded_features = encoder.transform(needed_features)

    return classifier.predict(encoded_features)



def get_lifetime_disqualifier(df: pd.DataFrame) -> pd.Series:
    numbered_charges = df.groupby(['person_id', 'HearingDate'])['HearingDate'].rank(method='first')
    return numbered_charges > 2


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


def get_sameday_disqualifier(df: pd.DataFrame) -> pd.Series:
    df = df.copy()
    df['is_not_auto'] = ~df['expungability'].isin(['Automatic', 'Automatic (pending)'])
    return df.groupby(['person_id', 'HearingDate'])['is_not_auto'].transform('any')


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


def classify_in_parallel(ddf: dd.DataFrame, config: ExpungeConfig) -> dd.DataFrame:
    ddf['expungability'] = ddf.map_partitions(
        classify_features,
        meta=pd.Series(dtype=str)
    )

    if config.lifetime_rule:
        ddf = apply_lifetime_rule(ddf)
    else:
        ddf['lifetime_disqualifier'] = None

    if config.sameday_rule:
        ddf = apply_sameday_rule(ddf)
    else:
        ddf['sameday_disqualifier'] = None
    
    return ddf
