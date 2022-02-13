import logging
from typing import Tuple
from functools import lru_cache

import pandas as pd
from sklearn import tree
from sklearn.preprocessing import OneHotEncoder


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
