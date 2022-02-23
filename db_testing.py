# %%
from pipeline.database import *
# %%
ddf = dd.read_sql_table(
    table='expunge',
    index_col='person_id',
    uri=DATABASE_URI
)
# %%
