import quixstreams as qx
import os
import redis
import pandas as pd
import humanize
from rocksdict import Rdict

# make sure the state dir exists
if not os.path.exists("state"):
    os.makedirs("state")

# rocksDb is used to hold state, state.dict is in the `state` folder which is maintained for us by Quix
# so we just init the rocks db using `state.dict` which will be loaded from the file system if it exists
db = Rdict("state/state.dict")

# Quix injects credentials automatically to the client.
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Use Input topic to stream data in our service
consumer_topic = client.get_topic_consumer(os.environ["input"])

# Create the redis client, to store the aggregated data
r = redis.Redis(
    host=os.environ['redis_host'],
    port=os.environ['redis_port'],
    password=os.environ['redis_password'],
    decode_responses=True)

# Default value for the last hour data, will be initialized from state store
if "last_hour_data" not in db.keys():
    print("Initialize last_hour_data in state store")
    db["last_hour_data"] = pd.DataFrame()
last_hour_data = db["last_hour_data"]

if "eight_hours_aggregation" not in db.keys():
    print("Initialize eight_hours_aggregation in state store")
    db["eight_hours_aggregation"] = pd.DataFrame()
eight_hours_aggregation = db["eight_hours_aggregation"]

last_sent_to_redis = pd.to_datetime(pd.Timestamp.now())

columns_from_enrich = ["timestamp", "original_timestamp", "userId", "ip", "userAgent", "productId",
                       "category", "title", "gender", "country", "deviceType", "age", "birthdate"]
buffer = pd.DataFrame(columns=columns_from_enrich)


def on_dataframe_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    """
    Callback called for each incoming dataframe. In this callback we will store
    the needed data (applying rolling windows accordingly) in the Quix state store
    and sending to Redis for later consumption.
    """
    global last_hour_data
    global last_sent_to_redis
    global buffer

    df["datetime"] = pd.to_datetime(df["timestamp"]) + pd.Timedelta(hours=2)
    buffer = pd.concat([buffer, df], ignore_index=True)

    print("------------------------------------------------")
    print("Data in buffer:", len(buffer))
    print("Data in last_hour_data:", len(last_hour_data))

    # If we have sent data to Redis in the last second, don't send again
    if last_sent_to_redis is not None and (pd.to_datetime(pd.Timestamp.now()) - last_sent_to_redis).seconds < 1:
        return

    # This method uses its own rolling window, so we only have to pass the buffer
    aggregate_eight_hours(buffer.copy())

    # Append data and discard data older than 1 hour
    last_hour_data = pd.concat([last_hour_data, buffer], ignore_index=True)
    last_hour_data = last_hour_data[
        last_hour_data["datetime"] > (pd.to_datetime(pd.Timestamp.now()) - pd.Timedelta(hours=1))]
    db["last_hour_data"] = last_hour_data
    print("Data in temp", len(last_hour_data))

    # Get average visits in the last 15 minutes
    calculate_visits_last_15min(last_hour_data.copy())

    # Get top viewed productId in the last hour, keep only productId, category and count
    calculate_products_last_hour(last_hour_data.copy())

    # Get latest 10 visitors details (date and time, ip and country)
    calculate_10_last_visitors(last_hour_data.copy())

    # Get category popularity in the last hour
    calculate_category_popularity(last_hour_data.copy())

    # Get device type popularity in the last 10 minutes
    calculate_device_popularity(last_hour_data.copy())

    # Send all data
    sorted_data = last_hour_data.sort_values(by='datetime', ascending=False)
    r.set("raw_data", sorted_data.head(100).to_json())

    # Clear buffer
    last_sent_to_redis = pd.to_datetime(pd.Timestamp.now())
    buffer = pd.DataFrame(columns=columns_from_enrich)


def calculate_device_popularity(df: pd.DataFrame):
    last_10_minutes = df[df["datetime"] > (pd.to_datetime(pd.Timestamp.now()) - pd.Timedelta(minutes=10))]
    last_10_minutes = last_10_minutes.drop_duplicates(subset=['userId'])
    last_10_minutes = last_10_minutes.groupby(['deviceType']).size().reset_index(name='count')

    total = last_10_minutes['count'].sum()
    mobile = last_10_minutes[last_10_minutes['deviceType'] == 'Mobile']['count'].sum() / total * 100
    tablet = last_10_minutes[last_10_minutes['deviceType'] == 'Tablet']['count'].sum() / total * 100
    desktop = last_10_minutes[last_10_minutes['deviceType'] == 'Desktop']['count'].sum() / total * 100
    other = 100.0 - mobile - tablet - desktop

    data = [["Device type", "Desktop", desktop],
            ["Device type", "Tablet", tablet],
            ["Device type", "Mobile", mobile],
            ["Device type", "Other", other]]

    device_type_popularity = pd.DataFrame(data, columns=["Device", "Device type", "Percentage"])
    print("Device type popularity:", [desktop, tablet, mobile, other])
    r.set("device_type_popularity", device_type_popularity.to_json())


def calculate_category_popularity(df: pd.DataFrame):
    category_popularity = df.groupby(['category']).size().reset_index(name='count')
    category_popularity = category_popularity.sort_values(by=['count'], ascending=False)
    print("Category popularity:", len(category_popularity))
    r.set("category_popularity", category_popularity.to_json())


def calculate_10_last_visitors(df: pd.DataFrame):
    latest_visitors = df.tail(10)[:]
    now = pd.Timestamp.now()
    latest_visitors["Date and Time"] = (now - pd.to_datetime(latest_visitors["datetime"])).dt.to_pytimedelta()
    latest_visitors["Date and Time"] = latest_visitors["Date and Time"].apply(humanize.naturaltime)
    latest_visitors = latest_visitors.sort_values(by='datetime', ascending=False)
    latest_visitors = latest_visitors[['Date and Time', 'ip', 'country']]
    print("Latest visitors:", len(latest_visitors))
    r.set("latest_visitors", latest_visitors.to_json())


def calculate_products_last_hour(df: pd.DataFrame):
    products_last_hour = df.groupby(['productId', 'category']).size().reset_index(name='count')
    products_last_hour = products_last_hour.sort_values(by=['count'], ascending=False).head(10)
    print("Products last hour:", len(products_last_hour))
    r.set("products_last_hour", products_last_hour.to_json())


def calculate_visits_last_15min(df: pd.DataFrame):
    last_15_minutes = df[df["datetime"] > (pd.to_datetime(pd.Timestamp.now()) - pd.Timedelta(minutes=15))]
    last_15_minutes = last_15_minutes.groupby(pd.Grouper(key='datetime', freq='1min')).size().reset_index(name='count')
    print("Last 15 minutes:", len(last_15_minutes))
    r.set("last_15_minutes", last_15_minutes.to_json())


def aggregate_eight_hours(df: pd.DataFrame):
    global eight_hours_aggregation

    df = df.groupby([pd.Grouper(key='datetime', freq='30min'), 'userId']).size().reset_index(name='count')

    # Add df_copy to eight_hours_aggregation. If the datetime is the same, add both counts
    eight_hours_aggregation = (pd.concat([eight_hours_aggregation, df])
                               .groupby(['datetime', 'userId']).sum().reset_index())

    # Store the eight_hours_aggregation in the state store
    db["eight_hours_aggregation"] = eight_hours_aggregation

    # Get sessions (unique users) in the last 8 hours (segmented by 30 mins)
    aggregated_df = eight_hours_aggregation.groupby(['datetime']).size().reset_index(name='count')

    # Store the aggregated_df in Redis
    r.set("sessions", aggregated_df.to_json())


def read_stream(consumer_stream: qx.StreamConsumer):
    # React to new data received from input topic.
    consumer_stream.timeseries.on_dataframe_received = on_dataframe_handler


if __name__ == "__main__":
    consumer_topic.on_stream_received = read_stream

    print("Listening to streams. Press CTRL-C to exit.")

    # Hook up to termination signal (for docker image) and CTRL-C
    # And handle graceful exit of the model.
    qx.App.run()
