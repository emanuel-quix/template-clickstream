# Top 10 products in the last hour

This service reads data from the 'enriched data' topic and, every second, publishes the top 10 products (based on the number of visits seen in the store) in the last 1 hour.

## Environment variables

The code sample uses the following environment variables:

- **input**: This is the input topic
- **ouput**: This is the output topic
