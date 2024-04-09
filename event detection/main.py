import os
from quixstreams import Application
from quixstreams.context import message_context
from behaviour_detector import BehaviourDetector
from datetime import timedelta, datetime

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

folder_path = 'state'
if os.path.exists(folder_path):
    def delete_folder(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                delete_folder(file_path)
        os.rmdir(folder_path)

    delete_folder(folder_path)

from uuid import uuid4

app = Application("transformation-v13"+str(uuid4()), auto_offset_reset="earliest", use_changelog_topics=False)

input_topic = app.topic(os.environ["input"])
output_topic = app.topic(os.environ["output"], value_serializer="quix_events")

sdf = app.dataframe(input_topic)
sdf = sdf.update(lambda row: print(f"{row}"))

# filter out messages that lack these columns or have None for them.
sdf = sdf[(sdf.contains("age")) & (sdf["age"].notnull())]
sdf = sdf[(sdf.contains("gender")) & (sdf["gender"].notnull())]
sdf = sdf[(sdf.contains("userId")) & (sdf["userId"].notnull())]
sdf = sdf[(sdf.contains("productId")) & (sdf["productId"].notnull())]
sdf = sdf[(sdf.contains("category")) & (sdf["category"].notnull())]

# sdf = sdf[(sdf["userId"] == 'short-friend-hurdling')]

behaviour_detector = BehaviourDetector()

# TODO key incoming data on user ID

def reducer(aggregated: dict, row: dict):
    
    if row is None: return aggregated

    # force gender to F
    row["gender"] = 'F'
    # force age to 30
    row["age"] = 30

    user_id = row["userId"]
    gender = row["gender"]
    age = row["age"]
    offer = aggregated["offer"]
    has_visited_clothing = aggregated['has_visited_clothing']
    has_visited_shoes = aggregated['has_visited_shoes']
    timestamp = aggregated['timestamp']

    # user_state = aggregated["state"]

    if row['category'] == 'shoes':
        # print(f'{user_id} shoes')
        has_visited_shoes = True

    if row['category'] == 'clothing':
        # print(f'{user_id} clothing')
        has_visited_clothing = True

    # if. has visited required categories and has not had an offer in this window:
    if has_visited_clothing and has_visited_shoes and offer is '':
        # determine which offer should be sent
        if gender == "F" and 25 <= age <= 35:
            offer = "offer2"
        if gender == "M" and 35 <= age <= 45:
            offer = "offer1"
    
    return {
        'has_visited_clothing': has_visited_clothing,
        'has_visited_shoes': has_visited_shoes,
        'timestamp': timestamp,
        'offer': offer
    }

    # transitioned = False
    # for transition in behaviour_detector.transitions[user_state]:
        
        
    #     if row['age'] == None or not row['age']:
    #         print('no age')
    #         # todo remove


    #     should_transition = transition["condition"](row, aggregated)
    #     if should_transition:
    #         aggregated["state"] = transition["next_state"]
    #         aggregated["rows"].append(row)
    #         aggregated["rows"] = aggregated["rows"][-3:]  # Keep only the last 3 rows

    #         transitioned = True

    #         log_text = f"[User {user_id} entered state {aggregated['state']}][Event: clicked {row['productId']}][Category: {row['category']}]"
    #         print(log_text)


    # # Reset to initial state if no transition was made
    # if not transitioned:
    #     # print(f"Resetting state to init for {user_id}")
    #     aggregated["state"] = "init"
    #     aggregated["rows"] = []
    # elif aggregated["state"] == "offer":
    #     aggregated["offer"] = "offer1" if gender == 'M' else "offer2"

    #     log_text = f"[User {user_id} triggered offer {aggregated['offer']}]"
    #     print(log_text)

    #     aggregated["state"] = "init"
    #     aggregated["rows"] = []
    #     behaviour_detector._special_offers_recipients.append((user_id, aggregated["offer"]))

    return aggregated


def initializer(row: dict):
    return {
        # "user_id": row["userId"],
        "has_visited_clothing": False,
        "has_visited_shoes": False,
        "timestamp": message_context().timestamp.milliseconds * 1000 * 1000,
        "offer": ''
    }

# def handle_new_row(agg, row):
#     agg['uid'] = row['userId']
#     agg['updated_thing'] = str(uuid4())
#     print(agg)
# sdf = sdf.hopping_window(timedelta(minutes=60), timedelta(minutes=30)).reduce(handle_new_row, lambda row: {'user': row['userId'], 'val':row}).current()


# sdf = sdf.hopping_window(timedelta(minutes=60), timedelta(minutes=30)).reduce(reducer, initializer).current()
sdf = sdf.tumbling_window(timedelta(minutes=60)).reduce(reducer, initializer).current()

def handle_windowed_data(row: dict):

    offer_recipients = behaviour_detector.get_special_offers_recipients()
    behaviour_detector.clear_special_offers_recipients()

    if offer_recipients:
        rtn = []
        for recipient in offer_recipients:
            rtn.append({
                        "userId": recipient[0],
                        "offer": recipient[1]
                       })
        return rtn

# sdf = sdf.apply(handle_windowed_data)

sdf = sdf.filter(lambda row: row['value']['offer'] is not '')
sdf = sdf.update(lambda row: print(f"{row}"))

sdf = sdf.apply(lambda row: {'Id': 'offer', 'Value': row["value"]['offer'], 'Timestamp': row['value']['timestamp']})
sdf = sdf.update(lambda row: print(f"{row}"))

# putput in Quix Event format
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)


# sdf = sdf.apply(lambda row: row["Speed"]) \
# .tumbling_window(timedelta(seconds=15)).mean().final() \
# .apply(lambda value: {'Timestamp': value['start'], 'AverageSpeed': value['value']})