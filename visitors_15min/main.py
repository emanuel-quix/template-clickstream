from quixstreams import Application
from datetime import timedelta
import os

# import the dotenv module to load environment variables from a file
from dotenv import load_dotenv
load_dotenv()


def main():
    app = Application.Quix(consumer_group="visitors-15min2")

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

    # this funciton will be called for every row received
    # it adds the userId from the row to state
    def row_processor(state: dict, row: dict):
        state[row['userId']] = 1
        return state

    # create a 15 minute hopping window with a 10 second step
    # pass each row to the row_processor function
    # take the final result of the window
    sdf = sdf.hopping_window(timedelta(minutes=15), timedelta(seconds=10)).reduce(row_processor, lambda r: row_processor({}, r)).final()
    # data produced by hopping window is: {'start': 1710861638000, 'end': 1710862538000, 'value': {'ABC123': 1}}

    # now we overwrite the 'value' column with the count of 
    # the length of the value dictionary. This gives us the 
    # number of unique visitors in the last 15 minutes
    # we will get an update every 1 second
    sdf["value"] = sdf["value"].apply(lambda u: len(u))
    # data after this stage is: {'start': 1710861639000, 'end': 1710862539000, 'value': 15}
    
    # print data after any stage of the pipeline to see what you're working with
    sdf = sdf.update(lambda row: print(row))

    # publish the data to a topic
    sdf = sdf.to_topic(output_topic)    

    # run the pipeline defined above
    app.run(sdf)

if __name__ == "__main__":
    main()