#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click


# In[14]:


dtype = {
    "LocationID" : "Int64",
    "Borough" : "string",
    "Zone" : "string",
    "service_zone" : "string"    
}


# In[28]:


pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'
target_table = 'taxi_service_zones'
chunksize = 100000
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc'
url = f'{prefix}/taxi_zone_lookup.csv'

engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

df = pd.read_csv(
    url,
    nrows=100,
    dtype=dtype,
)

df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')

df_iter = pd.read_csv(
    url,
    dtype=dtype,
    iterator=True,
    chunksize=chunksize
)


from tqdm.auto import tqdm
first = True
for df_chunk in tqdm(df_iter):
    if first:
        # Create table schema (no data)
        df_chunk.head(0).to_sql(
            name=target_table,
            con=engine,
            if_exists="replace"
        )
        first = False
        print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))




