from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("sqlite:///feast_store/data/driver_stats.db")
df = pd.read_parquet("feast_store/data/driver_stats.parquet")
df.created = pd.to_datetime(df.created)
df.datetime = pd.to_datetime(df.datetime)
df.to_sql('driver_stats', con=engine, if_exists='replace')

