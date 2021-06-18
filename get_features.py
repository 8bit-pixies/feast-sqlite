from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from feast import FeatureStore
from pyspark.sql import SparkSession
from datetime import timedelta

# just using this because I'm lazy...
spark = SparkSession \
    .builder \
    .appName("feast_sqlite_example") \
    .getOrCreate()

engine = create_engine("sqlite:///feast_store/data/driver_stats.db")
base_df = pd.read_sql_query("select * from driver_stats", engine)
# entity_df generally comes from upstream systems
#  
event_timestamp = pd.to_datetime(base_df.datetime.min()).date() + timedelta(days=1)
ttl = timedelta(days=-1)
entity_df = pd.DataFrame.from_dict({
    "driver_id": [1001, 1002, 1003, 1004],
    "event_timestamp": [pd.to_datetime(event_timestamp) for _ in range(4)]
})

store = FeatureStore(repo_path="feast_store")
​
# before we call this, we have to create things in the feature store to ingest...
start_timestamp = pd.to_datetime(event_timestamp)
end_timestamp = (start_timestamp + ttl)

df = pd.read_sql_query("select * from driver_stats", engine)
df.datetime = pd.to_datetime(df.datetime)
df.created = pd.to_datetime(df.created)
df = df[(df['datetime'] < start_timestamp) & (df['datetime'] > end_timestamp)]
sdf = spark.createDataFrame(df)
sdf.write.mode("overwrite").parquet("feast_store/data/driver_stats")

training_df = store.get_historical_features(
    entity_df=entity_df, 
    feature_refs = [
        'driver_hourly_stats:conv_rate',
        'driver_hourly_stats:acc_rate',
        'driver_hourly_stats:avg_daily_trips'
    ],
).to_df()
​
training_df.head()

# now if we do this with an entity out of range it should not work!
event_timestamp = pd.to_datetime(base_df.datetime.min()).date() + timedelta(days=10)
ttl = timedelta(days=-1)
entity_df = pd.DataFrame.from_dict({
    "driver_id": [1001, 1002, 1003, 1004],
    "event_timestamp": [pd.to_datetime(event_timestamp) for _ in range(4)]
})

training_df = store.get_historical_features(
    entity_df=entity_df, 
    feature_refs = [
        'driver_hourly_stats:conv_rate',
        'driver_hourly_stats:acc_rate',
        'driver_hourly_stats:avg_daily_trips'
    ],
).to_df()