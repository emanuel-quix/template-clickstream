# Top 10 products in the last hour

This service reads data from the 'enriched data' topic and publishes counts of the number of visitors using each device type over a specified period. Uses a [tumbling window](https://quix.io/docs/quix-streams/windowing.html).

## Environment variables

The code sample uses the following environment variables:

- **input**: This is the input topic
- **ouput**: This is the output topic
