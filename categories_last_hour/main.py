from quixstreams import Application
from datetime import timedelta
import os

# import the dotenv module to load environment variables from a file
from dotenv import load_dotenv
load_dotenv()


def main():
    app = Application(consumer_group="products-last-hour", use_changelog_topics=False, auto_offset_reset="latest")

    # Define the topic using the "output" environment variable
    input_topic_name = os.getenv("input", "")
    if input_topic_name == "":
        raise ValueError("The 'input' environment variable is required.")

    input_topic = app.topic(input_topic_name)

    output_topic_name = os.getenv("output", "")
    if output_topic_name == "":
        raise ValueError("The 'output' environment variable is required.")

    output_topic = app.topic(output_topic_name)

    # Create a StreamingDataFrame to process inbound data
    sdf = app.dataframe(input_topic)

    # this function will be called for all but the first message
    # it increments the stateful count of the category visited
    def reducer(aggregated: dict, row: dict):

        category = row['category']

        if category in aggregated:
            # This is the expected path / normal operation
            aggregated[category] += 1
        else:
            # handle instances where the item was not in state.
            aggregated[category] = 1
        return aggregated

    # this function will be called for only the first message
    # it initializes the state object with the an inital value
    def initializer(row: dict):
        return {row['category']: 1}
        
    # create a 1 hour hopping window with a 30 second step (this will emit data every 30 seconds)
    sdf = sdf.hopping_window(timedelta(minutes=60), timedelta(seconds=30)).reduce(reducer, initializer).final()

    # print data after any stage of the pipeline to see what you're working with
    sdf = sdf.update(lambda row: print(row))

    # publish the data to a topic
    sdf = sdf.to_topic(output_topic)

    # run the pipeline defined above
    app.run(sdf)

if __name__ == "__main__":
    main()