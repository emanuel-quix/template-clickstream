import quixstreams as qx
import os
import pandas as pd
import logging
from rlh import RedisStreamLogHandler

if 'window_minutes' not in os.environ:
    window_minutes = 30
else:
    window_minutes = int(os.environ['window_minutes'])

logger = logging.getLogger("States")


stream_log_handler = logging.StreamHandler()
stream_log_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_log_handler)

print(f"Creating redis log handler {os.environ['redis_host']}")

redis_log_handler = RedisStreamLogHandler(stream_name="state_logs",
                                          host=os.environ['redis_host'],
                                          port=int(os.environ['redis_port']),
                                          password=os.environ['redis_password'])
redis_log_handler.setLevel(logging.INFO)
logger.addHandler(redis_log_handler)


def check_time_elapsed(row, current_state):
    if len(current_state["rows"]) == 0:
        return True

    timestamp_row = row["timestamp"]
    timestamp_first_interaction = current_state["rows"][0]["timestamp"]
    window_ns = window_minutes * 60 * 1e9

    time_valid = timestamp_row - timestamp_first_interaction < window_ns

    if not time_valid:
        logger.debug(f"User {row['userId'][-4:]} state expired")

    return time_valid


class BehaviourDetector:
    columns = ["time", "timestamp", "userId", "category", "age", "ip", "gender", "productId", "offer"]
    visitor_columns = ["userId", "offer", "category", "productId"]

    transitions = {
        "init": [
            {
                "condition": lambda row, current_state: row["category"] == "clothing"
                                                        and ((row["gender"] == "M" and 35 <= row["age"] <= 45)
                                                             or (row["gender"] == "F" and 25 <= row["age"] <= 35)),
                "next_state": "clothes_visited",
            }
        ],
        "clothes_visited": [
            {
                "condition": lambda row, current_state: row["category"] == "shoes",
                "next_state": "shoes_visited"
            },
            {
                "condition": lambda row, current_state: row["category"] == "clothing",
                "next_state": "clothes_visited"
            }
        ],
        "shoes_visited": [
            {
                "condition": lambda row, current_state: row["category"] == "clothing"
                                                        and row["productId"] != current_state["rows"][0]["productId"],
                "next_state": "offer"
            },
            {
                "condition": lambda row, current_state: row["category"] == "clothing"
                                                        and row["productId"] == current_state["rows"][0]["productId"],
                "next_state": "clothes_visited"
            }
        ]
    }

    def __init__(self):
        self._special_offers_recipients = []

    # Method to process the incoming dataframe
    def process_dataframe(self, stream_consumer: qx.StreamConsumer, received_df: pd.DataFrame):
        print(received_df)

        for label, row in received_df.iterrows():
            user_id = row["userId"]
            print(f"Processing frame for {user_id}")

            # Filter out data that cannot apply for offers
            if "gender" not in row:
                logger.debug(f"User {user_id[-4:]} does not have gender, ignoring")
                continue

            if "age" not in row:
                logger.debug(f"User {user_id[-4:]} does not have age, ignoring")
                continue

            # Get state
            print(f"Getting state for {user_id}")
            user_state = stream_consumer.get_dict_state(user_id)
            print(f"Loaded state for {user_id}: {user_state}")


            # Initialize state if not present
            user_state["offer"] = "offer1" if row["gender"] == 'M' else "offer2"

            if "state" not in user_state:
                user_state["state"] = "init"

            if "rows" not in user_state:
                user_state["rows"] = []

            # Ignore page refreshes
            if user_state["rows"] and user_state["rows"][-1]["productId"] == row["productId"]:
                print(f"Ignoring page refresh for {user_id}")
                continue

            # Transition to next state if condition is met
            print(f"Applying transitions for {user_id}")
            transitioned = False
            for transition in self.transitions[user_state["state"]]:
                if transition["condition"](row, user_state) and check_time_elapsed(row, user_state):
                    print(f"Transitioned state for {user_id}")

                    user_state["state"] = transition["next_state"]
                    user_state["rows"].append(row)
                    transitioned = True
                    logger.info(f"[User {user_id[-4:]} entered state {user_state['state']}]"
                                f"[Event: clicked {row['productId']}]"
                                f"[Category: {row['category']}]")
                    break

            # Reset to initial state if no transition was made
            if not transitioned:
                print(f"Resetting state to init for {user_id}")

                user_state["state"] = "init"
                user_state["rows"] = []
                continue

            # Trigger offer
            if user_state["state"] == "offer":
                print(f"Triggering offer for {user_id}")

                logger.info(f"[User {user_id[-4:]} triggered offer {user_state['offer']}]")
                user_state["state"] = "init"
                user_state["rows"] = []
                self._special_offers_recipients.append((user_id, user_state["offer"]))

    def get_special_offers_recipients(self) -> list:
        """Return the recipients of the special offers."""
        return self._special_offers_recipients

    def clear_special_offers_recipients(self):
        """Clear the recipients of the special offers."""
        self._special_offers_recipients = []
