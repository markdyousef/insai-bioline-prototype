from bigquery import get_table, stream_data_bigquery
from pubsub import get_subscription
from google.cloud import bigquery


if __name__ == "__main__":
    # get pubsub subscription
    print("STREAMING")
    subscription = get_subscription()
    bq_client = bigquery.Client()
    print("TABLE")
    table = get_table(bq_client)
    print("STREAMING")
    stream_data_bigquery(bq_client, table, data_stream=subscription)