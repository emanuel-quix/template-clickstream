import os
from quixstreams import Application
from quixstreams.context import message_context
from datetime import timedelta
# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


from uuid import uuid4
app = Application.Quix("transformation-v1"+str(uuid4()), auto_offset_reset="earliest", use_changelog_topics=False)

input_topic = app.topic(os.environ["input"], value_deserializer='string')
output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

# put transformation logic here
# see docs for what you can do
# https://quix.io/docs/get-started/quixtour/process-threshold.html


fruit_sightings = {}

def reducer(aggregated: dict, row: dict):
    
    # uid should be set already on subsiquent rows for each message key
    if 'uid' in aggregated:
        uid = aggregated['uid'] 
    else:
        uid = str(uuid4())

    messages = aggregated['messages']
    messages.append(row)

    key  = str(message_context().key)
    

    # maintaint a separate count of the numbers of each fruit seen by this code.
    if key in fruit_sightings:
        fruit_sightings[key] += 1
    else:
        fruit_sightings[key] = 1

    print(fruit_sightings)

    # this is the new aggregated value
    return {
        'count': aggregated['count'] + 1,
        "uid": uid,
        "messages": messages,
        "context_key": key
    }

def initializer(row: dict):

    key = str(message_context().key)

    # maintaint a separate count of the numbers of each fruit seen by this code.
    if key in fruit_sightings:
        fruit_sightings[key] += 1
    else:
        fruit_sightings[key] = 1

    return {
        "count": 1,
        "messages": [row],
        "context_key": key
    }

sdf = sdf.hopping_window(timedelta(minutes=60), timedelta(minutes=5)).reduce(reducer, initializer).final()

sdf = sdf.update(lambda row: print(row))

# sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)