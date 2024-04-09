from quixstreams import Application, State
from datetime import timedelta
import os

# import the dotenv module to load environment variables from a file
from dotenv import load_dotenv
load_dotenv()


def main():
    app = Application.Quix(consumer_group="products-last-hour-2")

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
    # it increments the stateful count of the product
     # this funciton will be called for every row received
    # it adds the product from the row to state
    def reducer(aggregated: dict, row: dict):

        product_id = row['productId']

        # handle any instances where state is not a dictionary
        # Add the product id to state with the inital count of 1
        # if not isinstance(state, dict):
        #     print(f"State not dict: {state}")
        #     return {product_id: 1}

        if product_id in aggregated:
            # This is the expected path / normal operation
            aggregated[product_id] += 1
        else:
            # handle instances where the product ID was not in state.
            aggregated[product_id] = 1
        return aggregated

    # this function will be called for only the first message
    # it initializes the state object with the productId count (starting with 1)
    def initializer(row: dict):
        return {row['productId']: 1}
        
    # create a 1 hour hopping window with a 10 second step
    # pass each row to the reducer function
    # take the final result of the window, we get a value every 10 seconds
    sdf = sdf.hopping_window(timedelta(hours=1), timedelta(seconds=10)).reduce(reducer, initializer).final()
    # data produced by hopping window is: {'start': 1710861638000, 'end': 1710862538000, 'value': {'PRODUCT123': 1, 'PRODUCT567': 6}}

    def calculate_top_10_in_window(products: dict):
        # Sort the dictionary by count in descending order and take the top 10
        return dict(sorted(products.items(), key=lambda item: item[1], reverse=True)[:10])

    # now we overwrite the 'value' column with:
    # the top 10 most visited products
    sdf["value"] = sdf["value"].apply(calculate_top_10_in_window)
    # data after this stage is: {'start': 1710938386000, 'end': 1710941986000, 'value': {'VD55177927': 491, + 9 more }}
    
    # print data after any stage of the pipeline to see what you're working with
    sdf = sdf.update(lambda row: print(row))

    # publish the data to a topic
    sdf = sdf.to_topic(output_topic)    

    # run the pipeline defined above
    app.run(sdf)

if __name__ == "__main__":
    main()