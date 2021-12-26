import os
import logging

import sqlalchemy as sa
import pandas as pd
import numpy as np
import dask.dataframe as dd
from distributed import Client as DaskClient

from classify.db import DATABASE_URI, expunge_model
from classify.config import ExpungeConfig


logger = logging.getLogger(__name__)


def fetch_expunge_data(n_partitions: int) -> dd.DataFrame:
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


def build_convictions(ddf: dd.DataFrame) -> dd.DataFrame:
    
    def has_conviction(df: pd.DataFrame) -> pd.Series:
        conviction_map = (df['disposition']
                .apply(lambda x: x=='Conviction')
                .groupby('person_id')
                .any())

        return df.index.map(conviction_map)
    
    ddf['convictions'] = ddf.map_partitions(
        has_conviction,
        meta=pd.Series(dtype=bool)
    )
    return ddf


def fix_shifted_date_colums(ddf: dd.DataFrame, columns: list) -> dd.DataFrame:
    ...


def remove_unneeded_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    uneeded_columns = [
        'date_if_conviction', 
        'date_if_felony_conviction'
    ]
    return ddf.drop(uneeded_columns, axis=1)


def build_time_features(ddf: dd.DataFrame, config: ExpungeConfig) -> dd.DataFrame:

    def get_last_hearing_date(df: pd.DataFrame) -> pd.Series:
        return (
            df.groupby('person_id')['HearingDate']
              .shift(1)
        )
    ddf['last_hearing_date'] = ddf.map_partitions(
        get_last_hearing_date,
        meta=pd.Series(dtype='datetime64[ns]')
    )

    def get_conviction_dates(df: pd.DataFrame) -> pd.Series:
        return np.where(
            (df['disposition']=='Conviction'), 
            df['HearingDate'],
            np.datetime64('NaT')
        )
    ddf['date_if_conviction'] = ddf.map_partitions(
        get_conviction_dates,
        meta=pd.Series(dtype='datetime64[ns]')
    )

    def get_felony_conviction_dates(df: pd.DataFrame) -> pd.Series:
        return np.where(
            (df['chargetype']=='Felony'), 
            df['date_if_conviction'],
            np.datetime64('NaT')
        )
    ddf['date_if_felony_conviction'] = ddf.map_partitions(
        get_felony_conviction_dates,
        meta=pd.Series(dtype='datetime64[ns]')
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
    ddf['last_felony_conviction_date'] = ddf.map_partitions(
        get_last_felony_conviction_date,
        meta=pd.Series(dtype='datetime64[ns]')
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
    ddf['next_conviction_date'] = ddf.map_partitions(
        get_next_conviction_date,
        meta=pd.Series(dtype='datetime64[ns]')
    )
    ddf = remove_unneeded_columns(ddf)

    return ddf


def build_features(ddf: dd.DataFrame) -> dd.DataFrame:
    ...


def run_featurization(config: ExpungeConfig, n_partitions: int = None):
    logger.info("Initializing Dask distributed client")
    client = DaskClient()
    
    ddf = fetch_expunge_data(n_partitions)
    ddf = clean_data(ddf)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s - %(name)s.py]: %(message)s'
    )

    config = ExpungeConfig.from_yaml('classify/expunge_config.yaml')
    run_featurization(config)
