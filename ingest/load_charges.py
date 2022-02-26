# %%
import logging

from distributed import Client as DaskClient
import dask.dataframe as dd
import numpy as np

from db.utils import DATABASE_URI


logger = logging.getLogger(__name__)


# %%
def read_raw_data() -> dd.DataFrame:
    logger.info('Reading raw data into Dask')
    return dd.read_sql_table(
        'expunge',
        DATABASE_URI,
        index_col='person_id'
    )


# %%
def rename_columns(ddf: dd.DataFrame) -> dd.DataFrame:
    column_map = {
        'HearingDate': 'hearing_date',
        'CodeSection': 'code_section',
        'ChargeType': 'charge_type',
        'Class': 'charge_class',
        'DispositionCode': 'disposition_code',
        'Plea': 'plea',
        'Race': 'race',
        'Sex': 'sex',
        'fips': 'fips'
    }
    return ddf.rename(columns=column_map)


# %%
def filter_hearing_dates(ddf: dd.DataFrame) -> dd.DataFrame:
    ddf['hearing_date'] = ddf['hearing_date'].astype('datetime64[ns]')
    # TODO: Investigate valid reasons for future hearing dates. 
    return ddf[ddf['hearing_date'] <= np.datetime64('today')]


def standardize_race(ddf: dd.DataFrame) -> dd.DataFrame:
    race: dd.Series = ddf['race'].str.upper()
    ddf['race'] = (
        ddf['race']
            .mask(race.str.contains('BLACK'), 'Black')
            .mask(
                race.str.contains('WHITE')
                | race.str.contains('CAUCASIAN'),
                'White'
            )
            .mask(race.str.contains(
                # https://regex101.com/r/nJazne/1
                r"(?<![A-Z])ASIAN", regex=True),
                'Asian or Pacific Islander'
            )
            .mask(
                race.str.contains('AMERICAN INDIAN')
                | race.str.contains('NATIVE'),
                'American Indian or Alaskan Native'
            )
            .mask(
                race.str.contains('OTHER')
                | race.str.contains('MISSING')
                | race.str.contains('UNKNOWN'),
                'Unknown'
            )
    )
    return ddf


def fix_fips_codes(ddf: dd.DataFrame) -> dd.DataFrame:
    ddf['fips'] = (ddf['fips'].astype(str)
                              .str.pad(5, fillchar='0'))
    return ddf


def clean_charge_data(ddf: dd.DataFrame) -> dd.DataFrame:
    logger.info("Cleaning raw data")
    return (
        ddf.pipe(rename_columns)
           .pipe(filter_hearing_dates)
           .pipe(standardize_race)
           .pipe(fix_fips_codes)
    )

# %%
def main():
    ddf = dd.read_sql_table(
        'expunge',
        DATABASE_URI,
        index_col='person_id'
    )
    ddf = clean_charge_data(ddf)



# %%
if __name__ == '__main__':
    DaskClient()
    main()
