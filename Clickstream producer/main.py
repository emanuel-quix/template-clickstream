import os
from quixstreams import Application
import time
from datetime import datetime
import json
import re
import csv
import os

# import the dotenv module to load environment variables from a file
from dotenv import load_dotenv
load_dotenv(override=False)

# True = keep original timings.
# False = No delay! Speed through it as fast as possible.
keep_timing = "keep_timing" in os.environ and os.environ["keep_timing"] == "1"

# If the process is terminated on the command line or by the container
# setting this flag to True will tell the loops to stop and the code
# to exit gracefully.
shutting_down = False

# Create an Application.
app = Application.Quix()

# Define the topic using the "output" environment variable
topic_name = os.getenv("output", "")
if topic_name == "":
    raise ValueError("The 'output' environment variable is required. This is the output topic that data will be published to.")

topic = app.topic(topic_name)

# Create a pre-configured Producer object.
# Producer is already setup to use Quix brokers.
# It will also ensure that the topics exist before producing to them if
# Application.Quix is initiliazed with "auto_create_topics=True" (the default).
producer = app.get_producer()

# counters for the status messages
row_counter = 0
published_total = 0


def publish_row(row):
    global row_counter
    global published_total

    # add a new timestamp column with the current data and time
    row['timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')    

    # Serialize row_data to a JSON string
    json_data = json.dumps(row)
    # Encode the JSON string to a byte array
    serialized_value = json_data.encode('utf-8')
    
    print(row)
    producer.produce(
        topic=topic.name,
        key="ClickStream",
        value=serialized_value
    )

    row_counter += 1

    if row_counter == 10:
        row_counter = 0
        published_total += 10
        print(f"Published {published_total} rows")


def get_product_id(url):
    match = re.search(r"http://www.acme.com/(\w+)/(\w+)", url)

    if match is not None:
        return match.group(2)

    return url


def main(csv_file):
    global shutting_down

    print("TSV file loading.")
    with open(csv_file, newline='') as file:
        # Read the first line to get the headers
        headers = next(csv.reader(file, delimiter='\t'))
        
        # Rename specific headers by index or by matching content
        headers = [header if header != "Visitor Unique ID" else "userId" for header in headers]
        headers = [header if header != "IP Address" else "ip" for header in headers]
        headers = [header if header != "29" else "userAgent" for header in headers]
        headers = [header if header != "Unix Timestamp" else "original_timestamp" for header in headers]

        # Now use the modified headers for the DictReader
        file.seek(0)  # Reset file read position to the beginning
        next(file)  # Skip the original headers line
        reader = csv.DictReader(file, fieldnames=headers, delimiter='\t')

        print("File loaded.")

        # If shutdown has been requested, exit the loop.
        while not shutting_down:
            # Iterate over the rows and send them to the API
            for row in reader:
                # If shutdown has been requested, exit the loop.
                if shutting_down:
                    break

                # Create a dictionary that includes both column headers and row values
                row_data = {header: row[header] for header in headers if header in row}

                # Preprocess the row (e.g., strip "{}" from userId, get productId)
                row_data['userId'] = row['userId'].strip("{}")
                row_data['productId'] = get_product_id(row['Product Page URL'])

                publish_row(row_data)

                if not keep_timing:
                    # Don't want to keep the original timing or no timestamp? That's ok, just sleep for 200ms
                    time.sleep(0.2)
                else:
                    # Delay sending the next row if it exists
                    # The delay is calculated using the original timestamps and ensure the data
                    # is published at a rate similar to the original data rates
                    # This part needs to be adjusted as we no longer use pandas for datetime operations
                    next_row = next(reader, None)
                    if next_row:
                        current_timestamp = int(row['original_timestamp'])
                        next_timestamp = int(next_row['original_timestamp'])
                        time_difference = next_timestamp - current_timestamp

                        # handle < 0 delays
                        if time_difference < 0:
                            time_difference = 0

                        if time_difference > 10:
                            time_difference = 10

                        time.sleep(time_difference)
                    # Make sure to go back one row since we read ahead
                    file.seek(reader.line_num)


if __name__ == "__main__":
    try:
        main('omniture-logs.tsv')
    except KeyboardInterrupt:
        # set the flag to True to stop the loops as soon as possible.
        shutting_down = True
        print("Exiting.")
