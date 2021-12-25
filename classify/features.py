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

    ddf['codesection'] = ddf.map_partitions(
        lambda df: df.apply(assign_code_section, axis=1),
        meta=pd.Series(dtype=str)
    )



def build_convictions(ddf: dd.DataFrame) -> dd.DataFrame:
    ...


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
