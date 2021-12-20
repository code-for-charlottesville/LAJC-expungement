# %%
# Python Native Libraries
import os

# 3rd Party Libraries
import sqlalchemy as sa
from sqlalchemy.sql import select
from sqlalchemy import (
    Table, 
    Column, 
    Integer, 
    String, 
    MetaData, 
    DateTime,
    or_
)
import pandas as pd
import numpy as np
import dask.dataframe as dd
from distributed import Client as DaskClient

from classify.db import DATABASE_URI

# %%



def engineer_features():
    client = DaskClient()
    

# %%


# %%
if __name__ == '__main__':
    engineer_features()


