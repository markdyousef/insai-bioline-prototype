from google.cloud import pubsub_v1
from bigquery import get_table, stream_data_bigquery
from pubsub import get_subscription


if __name__ == "__main__":
    # get pubsub subscription
    subscription = get_subscription()
    bq_client = bigquery.Client()
    table = get_table(bq_client)
    stream_data_bigquery(bq_client, table, data_stream=subscription)