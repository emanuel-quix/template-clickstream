import os
import random
from quixstreams import Application

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application.Quix(use_changelog_topics=False)

output_topic = app.topic(os.environ["output"])

fruit = ['orange', 'apple', 'kiwi', 'pear', 'grape']

while True:
    with app.get_producer() as producer:
        random_fruit = random.randint(0, len(fruit) - 1)  # Generate a random index
        fruity_number = random.randint(0, 1000)  # Generate a random number between 0 and 1000
        
        producer.produce(
            topic=output_topic.name,
            value=f'Your fruity number is {fruity_number}',
            key=fruit[random_fruit]
        )