import os
import logging
from typing import Union

import sqlalchemy as sa
import pandas as pd
import numpy as np
import dask.dataframe as dd
from distributed import Client as DaskClient

from classify.db import DATABASE_URI, expunge_model
from classify.config import ExpungeConfig


logger = logging.getLogger(__name__)


def fetch_expunge_data(n_partitions: Union[int,None] = None) -> dd.DataFrame:
    """Fetches expungement records and loads them into a Dask DataFrame. 

    Args:
        n_partitions: The number of underlying Pandas DataFrames to partition
            the table into (partitioned by 'person_id'). If None, we allow 
            Dask to choose automatically. 
    """

    query = (
        sa.sql.select(expunge_model)
            .order_by(
                expunge_model.c.person_id,
                expunge_model.c.HearingDate
            )
    )

    dask_types = {
        'HearingDate': 'datetime64[ns]',
        'CodeSection': str,
        'ChargeType': str,
        'Class': str,
        'DispositionCode': str,
        'Plea': str,
        'Race': str,
        'Sex': str,
        'fips': 'int64'
    }
    meta_frame = pd.DataFrame(columns=dask_types.keys()).astype(dask_types)

    kwargs = {'npartitions': n_partitions} if n_partitions else {}

    logger.info(f"Reading from table: {expunge_model.name}")
    logger.info(f"Loading into {n_partitions} partitions")
    return dd.read_sql_table(
        table=query,
        index_col='person_id',
        uri=DATABASE_URI,
        meta=meta_frame,
        **kwargs
    )


def remove_invalid_dispositions(ddf: dd.DataFrame) -> dd.DataFrame:
    return ddf[
        (~ddf['DispositionCode'].isna())
        & (ddf['DispositionCode'].isin([
            'Guilty',
            'Guilty In Absentia',
            'Dismissed',
            'Nolle Prosequi',
            'Not Guilty',
            'Not Guilty/Acquitted',
            'No Indictment Presented',
            'Not True Bill',
            'Dismissed/Other'
        ]))
    ]


def clean_data(ddf: dd.DataFrame) -> dd.DataFrame:
    ddf['CodeSection'] = ddf['CodeSection'].fillna('MISSING')
    ddf = remove_invalid_dispositions(ddf)
    return ddf


def build_disposition(ddf: dd.DataFrame) -> dd.DataFrame:
    """Group disposition codes into relevant categories for classification. 

    The possible relevant categories for expungement classification are: 
    - Conviction
    - Dismissed
    - Deferral Dismissal
    """
    ddf['disposition'] = ddf['DispositionCode'].replace({
        'Nolle Prosequi': 'Dismissed',
        'No Indictment Presented': 'Dismissed',
        'Not True Bill': 'Dismissed',
        'Dismissed/Other': 'Dismissed',
        'Not Guilty': 'Dismissed',
        'Not Guilty/Acquitted': 'Dismissed',
        'Guilty In Absentia': 'Conviction',
        'Guilty': 'Conviction',
    })

    deferral_pleas = [
        'Alford',
        'Guilty',
        'Nolo Contendere'
    ]
    deferral_mask = (
        (ddf['Plea'].isin(deferral_pleas))
        & (ddf['disposition']=='Dismissed')
    )
    ddf['disposition'] = ddf['disposition'].mask(deferral_mask, 'Deferral Dismissal')

    return ddf


def build_chargetype(ddf: dd.DataFrame) -> dd.DataFrame:
    ddf['chargetype'] = ddf['ChargeType']
    return ddf


def build_codesection(ddf: dd.DataFrame, config: ExpungeConfig) -> dd.DataFrame:
    """Categorize Virginia legal code section.

    Code sections are grouped into categories based on the section of Virginia
    code that applies to their eligibility for expungement. 
    """
    def assign_code_section(row):
        if (
            row['CodeSection'] in config.covered_sections_a 
            and row['disposition']=='Deferral Dismissal'
        ):
            return 'covered in 19.2-392.6 - A'
        
        elif (
            row['CodeSection'] in config.covered_sections_b
            or (
                row['CodeSection'] in config.covered_sections_b_misdemeanor
                and row['chargetype']=='Misdemeanor'
            )
        ):
            return 'covered in 19.2-392.6 - B'
        
        elif row['CodeSection'] in config.excluded_sections_twelve:
            return 'excluded by 19.2-392.12'
        
        else:
            return 'covered elsewhere'

    ddf['codesection'] = ddf.apply(
        assign_code_section, 
        axis=1,
        meta=pd.Series(dtype=str)
    )
    return ddf


def has_conviction(df: pd.DataFrame) -> pd.Series:
    conviction_map = (df['disposition']
            .apply(lambda x: x=='Conviction')
            .groupby('person_id')
            .any())

    return df.index.map(conviction_map)


def build_convictions(ddf: dd.DataFrame) -> dd.DataFrame:
    """Adds column 'convictions' to Dask DataFrame.

    'convictions' is assigned to True for a given record if
    the person_id for that record has ANY other records that
    resulted in a conviction. 
    """
    ddf['convictions'] = ddf.map_partitions(
        has_conviction,
        meta=pd.Series(dtype=bool)
    )
    return ddf


def get_last_hearing_date(df: pd.DataFrame) -> pd.Series:
    return (
        df.groupby('person_id')['HearingDate']
            .shift(1)
    )


def get_conviction_dates(df: pd.DataFrame) -> pd.Series:
    return np.where(
        (df['disposition']=='Conviction'), 
        df['HearingDate'],
        np.datetime64('NaT')
    )


def get_felony_conviction_dates(df: pd.DataFrame) -> pd.Series:
    return np.where(
        (df['chargetype']=='Felony'), 
        df['date_if_conviction'],
        np.datetime64('NaT')
    )


def get_last_felony_conviction_date(df: pd.DataFrame) -> pd.Series:
    return (
        df['date_if_felony_conviction']
            .groupby('person_id')
            .shift(1)
            .groupby('person_id')
            .ffill()
            .fillna(pd.NaT)
    )


def get_next_conviction_date(df: pd.DataFrame) -> pd.Series:
    return (
        df['date_if_conviction']
            .groupby('person_id')
            .shift(-1)
            .groupby('person_id')
            .bfill()
            .fillna(pd.NaT)
    )


def fix_shifted_sameday_dates(
    df: pd.DataFrame, 
    fix_column: str, 
    is_backward_facing: bool = True
) -> pd.Series:
    grouped_dates = df.groupby(['person_id','HearingDate'])[fix_column]

    if is_backward_facing:
        transformation = lambda df: df.min(skipna=False)
    else:
        transformation = lambda df: df.max(skipna=False)
        
    return grouped_dates.transform(transformation)


def fix_shifted_date_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """Standardizes differing date aggregations within one person_id. 
    """
    backward_facing_cols = [
        'last_hearing_date',
        'last_felony_conviction_date',
    ]
    for column in backward_facing_cols:
        ddf[column] = ddf.map_partitions(
            fix_shifted_sameday_dates,
            fix_column=column,
            meta=pd.Series(dtype='datetime64[ns]')
        )

    forward_facing_cols = [
        'next_conviction_date'
    ]
    for column in forward_facing_cols:
        ddf[column] = ddf.map_partitions(
            fix_shifted_sameday_dates,
            fix_column=column,
            is_backward_facing=False,
            meta=pd.Series(dtype='datetime64[ns]')
        )

    return ddf


def remove_unneeded_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    uneeded_columns = [
        'date_if_conviction', 
        'date_if_felony_conviction'
    ]
    return ddf.drop(uneeded_columns, axis=1)


def build_date_features(ddf: dd.DataFrame) -> dd.DataFrame:
    def build_date_feature(func, meta=pd.Series(dtype='datetime64[ns]')):
        return ddf.map_partitions(func, meta=meta)

    column_builder_map = {
        'last_hearing_data': get_last_hearing_date,
        'date_if_conviction': get_conviction_dates,
        'date_if_felony_conviction': get_felony_conviction_dates,
        'last_felony_conviction_date': get_last_felony_conviction_date,
        'next_conviction_date': get_next_conviction_date,        
    }
    for col, func in column_builder_map.items():
        ddf[col] = build_date_feature(func)

    ddf = remove_unneeded_columns(ddf)
    ddf = fix_shifted_date_columns(ddf)

    return ddf


def build_timedelta_features(
    ddf: dd.DataFrame, 
    config: ExpungeConfig
) -> dd.DataFrame:
    ddf['days_since_last_hearing'] = ddf['HearingDate'] - ddf['last_hearing_date']
    ddf['days_until_next_conviction'] = ddf['next_conviction_date'] - ddf['HearingDate']
    ddf['days_since_last_felony_conviction'] = ddf['HearingDate'] - ddf['last_felony_conviction_date']

    return ddf


def build_features(ddf: dd.DataFrame, config: ExpungeConfig) -> dd.DataFrame:
    return (
        ddf.pipe(build_disposition)
           .pipe(build_chargetype)
           .pipe(build_codesection)
           .pipe(build_convictions)
           .pipe(build_date_features)
           .pipe(build_timedelta_features, config)
    )


def run_featurization(config: ExpungeConfig, n_partitions: int = None):
    logger.info("Initializing Dask distributed client")
    DaskClient()
    
    ddf = fetch_expunge_data(n_partitions)
    ddf = clean_data(ddf)

    ddf = build_features(ddf, config)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s - %(name)s.py]: %(message)s'
    )

    config = ExpungeConfig.from_yaml('classify/expunge_config.yaml')
    run_featurization(config)
