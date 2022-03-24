import logging
from uuid import uuid4

import pandas as pd
import numpy as np
import dask.dataframe as dd

from expunge.config_parser import RunConfig


logger = logging.getLogger(__name__)


def remove_invalid_dispositions(ddf: dd.DataFrame) -> dd.DataFrame:
    return ddf[
        (~ddf['disposition_code'].isna())
        & (ddf['disposition_code'].isin([
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
    # This line is a carryover from the old classification engine.
    # Should be deleted when we can ensure it is not being used downstream
    ddf['code_section_category'] = ddf['code_section'].fillna('MISSING')

    ddf = remove_invalid_dispositions(ddf)
    return ddf


def build_disposition_category(ddf: dd.DataFrame) -> dd.DataFrame:
    """Group disposition codes into relevant categories for classification. 

    The possible relevant categories for expungement classification are: 
    - Conviction
    - Dismissed
    - Deferral Dismissal
    """
    ddf['disposition_category'] = ddf['disposition_code'].replace({
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
        (ddf['plea'].isin(deferral_pleas))
        & (ddf['disposition_category']=='Dismissed')
    )
    ddf['disposition_category'] = ddf['disposition_category'].mask(deferral_mask, 'Deferral Dismissal')

    return ddf


def build_charge_category(ddf: dd.DataFrame) -> dd.DataFrame:
    # Currently, charge_category is equivalent to the without
    # any adjustments. However, it is likely that some cleaning
    # and categorization will be necessary here in the future. 
    ddf['charge_category'] = ddf['charge_type']
    return ddf


def build_code_section_category(ddf: dd.DataFrame, config: RunConfig) -> dd.DataFrame:
    """Categorize Virginia legal code section.

    Code sections are grouped into categories based on the section of Virginia
    code that applies to their eligibility for expungement. 
    """
    def assign_code_section(row):
        if (
            row['code_section'] in config.covered_sections_a 
            and row['disposition_category']=='Deferral Dismissal'
        ):
            return 'covered in 19.2-392.6 - A'
        
        elif (
            row['code_section'] in config.covered_sections_b
            or (
                row['code_section'] in config.covered_sections_b_misdemeanor
                and row['charge_category']=='Misdemeanor'
            )
        ):
            return 'covered in 19.2-392.6 - B'
        
        elif row['code_section'] in config.excluded_sections_twelve:
            return 'excluded by 19.2-392.12'
        
        else:
            return 'covered elsewhere'

    ddf['code_section_category'] = ddf.apply(
        assign_code_section, 
        axis=1,
        meta=pd.Series(dtype=str)
    )
    return ddf


def has_conviction(df: pd.DataFrame) -> pd.Series:
    conviction_map = (df['disposition_category']
            .apply(lambda x: x=='Conviction')
            .groupby('person_id')
            .any())

    return df.index.map(conviction_map)


def build_has_conviction(ddf: dd.DataFrame) -> dd.DataFrame:
    """Adds column 'has_conviction' to Dask DataFrame.

    'has_conviction' is assigned to True for a given record if
    the person_id for that record has ANY other records that
    resulted in a conviction. 
    """
    ddf['has_conviction'] = ddf.map_partitions(
        has_conviction,
        meta=pd.Series(dtype=bool)
    )
    return ddf


def get_last_hearing_date(df: pd.DataFrame) -> pd.Series:
    return (
        df.groupby('person_id')['hearing_date']
            .shift(1)
    )


def get_conviction_dates(df: pd.DataFrame) -> pd.Series:
    return np.where(
        (df['disposition_category']=='Conviction'), 
        df['hearing_date'],
        np.datetime64('NaT')
    )


def get_felony_conviction_dates(df: pd.DataFrame) -> pd.Series:
    return np.where(
        (df['charge_category']=='Felony'), 
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
    """Ensure dates features are consistent within one person_id and hearing_date
    
    Args:
        df: Single Dask DataFrame partition.
        fix_column: The date feature column to fix.
        is_backward_facing: True if the date feature concerns past records, such
            as most recent previous hearing, etc. False if feature concerns more
            recent records.
    """
    grouped_dates = df.groupby(['person_id', 'hearing_date'])[fix_column]
    idx = 0 if is_backward_facing else -1
    return grouped_dates.transform('nth', idx)


def fix_shifted_date_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    """Standardizes differing date aggregations within one person_id."""
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


def build_date_features(ddf: dd.DataFrame) -> dd.DataFrame:
    def build_date_feature(func, meta=pd.Series(dtype='datetime64[ns]')):
        return ddf.map_partitions(func, meta=meta)

    column_builder_map = {
        'last_hearing_date': get_last_hearing_date,
        'date_if_conviction': get_conviction_dates,
        'date_if_felony_conviction': get_felony_conviction_dates,
        'last_felony_conviction_date': get_last_felony_conviction_date,
        'next_conviction_date': get_next_conviction_date,        
    }
    for col, func in column_builder_map.items():
        ddf[col] = build_date_feature(func)

    ddf = fix_shifted_date_columns(ddf)

    return ddf


def calculate_timedeltas(ddf: dd.DataFrame, config: RunConfig) -> dd.DataFrame:
    """Calculate any needed timedelta columns for later comparisons"""
    ddf['last_hearing_delta'] = ddf['hearing_date'] - ddf['last_hearing_date']
    ddf['last_felony_conviction_delta'] = ddf['hearing_date'] - ddf['last_felony_conviction_date']
    ddf['next_conviction_delta'] = ddf['next_conviction_date'] - ddf['hearing_date']
    ddf['from_present_delta'] = -(ddf['hearing_date'] - np.datetime64(config.cutoff_date))

    return ddf


def build_timedelta_disqualifiers(ddf: dd.DataFrame, config: RunConfig) -> dd.DataFrame:
    """Build boolean features that depend on timedelta comparisons"""
    delta_since_arrest = np.timedelta64(config.years_since_arrest, 'Y')
    delta_since_felony = np.timedelta64(config.years_since_felony, 'Y')
    delta_until_conviction_after_misdemeanor = np.timedelta64(config.years_until_conviction_after_misdemeanor, 'Y')
    delta_until_conviction_after_felony = np.timedelta64(config.years_until_conviction_after_felony, 'Y')
    delta_until_conviction_after_misdemeanor = np.timedelta64(config.years_until_conviction_after_misdemeanor, 'Y')
    delta_until_conviction_after_felony = np.timedelta64(config.years_until_conviction_after_felony, 'Y')

    ddf['arrest_disqualifier'] = ddf['last_hearing_delta'] < delta_since_arrest
    ddf['felony_conviction_disqualifier'] = ddf['last_felony_conviction_delta'] < delta_since_felony
    ddf['next_conviction_disqualifier_after_misdemeanor'] = ddf['next_conviction_delta'] < delta_until_conviction_after_misdemeanor
    ddf['next_conviction_disqualifier_after_felony'] = ddf['next_conviction_delta'] < delta_until_conviction_after_felony
    ddf['pending_after_misdemeanor'] = ddf['from_present_delta'] < delta_until_conviction_after_misdemeanor
    ddf['pending_after_felony'] = ddf['from_present_delta'] < delta_until_conviction_after_felony

    return ddf


def convert_timedeltas_to_days(ddf: dd.DataFrame) -> dd.DataFrame:
    """Convert timedelta columns to days to allow loading to DB as integers."""
    timedelta_cols = [col for col in ddf.columns if col.endswith('_delta')]
    for col in timedelta_cols:
        ddf[col] = ddf[col].dt.days
    return ddf


def build_timedelta_features(
    ddf: dd.DataFrame, 
    config: RunConfig
) -> dd.DataFrame:
    """Builds features for time differences between records or from present."""
    return (
        ddf.pipe(calculate_timedeltas, config)
           .pipe(build_timedelta_disqualifiers, config)
           .pipe(convert_timedeltas_to_days)
    )


def any_true_per_person_id(df: pd.DataFrame, column: str) -> pd.Series:
    """Determines whether any value of a column is true across a person_id
    
    Example usage: If a person_id has 4 records, did even 1 of them result
        in a conviction? Then mark all as 'True', otherwise 'False'. 
    """
    return (
        df[column]
          .groupby('person_id')
          .any()
    )


def build_class_features(ddf: dd.DataFrame) -> dd.DataFrame:
    """Builds features related to Class 1-4 felony convictions"""
    felony_conviction_mask = (
        (ddf['charge_category']=='Felony')
        & (ddf['disposition_category']=='Conviction')
    )
    ddf['is_class_1_or_2'] = (
        felony_conviction_mask
        & (ddf['charge_class'].isin(['1', '2']))
    )
    ddf['is_class_3_or_4'] = (
        felony_conviction_mask
        & (ddf['charge_class'].isin(['3', '4']))
    )

    return_meta = pd.Series(dtype=bool)
    ddf['has_class_1_or_2'] = ddf.map_partitions(
        any_true_per_person_id,
        column='is_class_1_or_2',
        meta=return_meta
    )
    ddf['has_class_3_or_4'] = ddf.map_partitions(
        any_true_per_person_id,
        column='is_class_3_or_4',
        meta=return_meta
    )

    return ddf


def remove_unneeded_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    uneeded_columns = [
        'date_if_conviction', 
        'date_if_felony_conviction',
        'is_class_1_or_2',
        'is_class_3_or_4'
    ]
    return ddf.drop(uneeded_columns, axis=1)


def append_run_id(ddf: dd.DataFrame, config: RunConfig) -> dd.DataFrame:
    """Adds unique ID for querying results of classification pipeline runs"""
    run_id = str(uuid4()) if config.id == 'randomize' else config.id
    ddf['run_id'] = run_id
    return ddf


def build_features(ddf: dd.DataFrame, config: RunConfig) -> dd.DataFrame:
    logger.info("Building Dask task graph for feature construction")
    return (
        ddf.pipe(build_disposition_category)
           .pipe(build_charge_category)
           .pipe(build_code_section_category, config)
           .pipe(build_has_conviction)

           .pipe(build_date_features)
           .pipe(build_timedelta_features, config)
           .pipe(build_class_features)
           
           .pipe(remove_unneeded_columns)
           .pipe(append_run_id, config)
    )
