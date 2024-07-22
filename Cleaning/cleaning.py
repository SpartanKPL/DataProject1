import numpy as np
import pandas as pd
import sqlalchemy as sa
import os

connection_string = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server=127.0.0.1;'
    'Database=master;'
    'UID=sa;'
    'PWD=Password1!;'
    'Trusted_Connection=no;'
)
connection_url = sa.engine.URL.create(
    "mssql+pyodbc",
    query=dict(odbc_connect=connection_string)
)
engine = sa.create_engine(connection_url, fast_executemany=True)

df = pd.read_csv("C:\\Users\\Kyrun\\Desktop\\Work\\Data402\\Preassignment\\kaggleconnect\\netflix-shows\\netflix_titles.csv")

df = df.drop("director", axis = 1)

df = df.dropna()

df = df.drop("description", axis = 1)

df["country"] = df["country"].str.strip()
df["country"] = df["country"].str.split(pat=",")
df["cast"] = df["cast"].str.strip()
df["cast"] = df["cast"].str.split(pat=",")
df["listed_in"] = df["listed_in"].str.strip()
df["listed_in"] = df["listed_in"].str.split(pat=",")
df = df.reset_index()
df = df.drop("index",axis=1)

df2= pd.DataFrame(df["cast"].tolist()).add_prefix('actor_')
df3 = pd.DataFrame(df["listed_in"].tolist()).add_prefix("genre_")
df4 = pd.DataFrame(df["country"].tolist()).add_prefix("country_")

df = pd.concat([df,df2], axis = 1)
df = pd.concat([df,df3], axis = 1)
df= pd.concat([df,df4], axis = 1)

df = df.drop("cast", axis = 1)
df = df.drop("listed_in", axis = 1)
df = df.drop("country", axis = 1)

df["date_added"] = pd.to_datetime(df["date_added"], format="mixed")

def clean_actor_whitespace():
    for times in range (0, 50):
        (df[f"actor_{times}"]) = df[f"actor_{times}"].str.strip()
    return

def clean_genre_whitespace():
    for times in range (0, 3):
        (df[f"genre_{times}"]) = df[f"genre_{times}"].str.strip()
    return

def clean_country_whitespace():
    for times in range (0, 8):
        (df[f"country_{times}"]) = df[f"country_{times}"].str.strip()
    return

df
clean_actor_whitespace()

clean_genre_whitespace()

clean_country_whitespace()

def insert_into_sql(dataframe: pd.DataFrame, engine: sa.engine, tablename: str) -> None:
    dataframe.to_sql(tablename, engine, schema="dbo", if_exists="replace", index=False)
    return

df

insert_into_sql(df,engine,"testing")
