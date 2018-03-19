export GOOGLE_APPLICATION_CREDENTIALS="./service-account.json"
export PROJECT_ID=knowledge-prototype
export PUBSUB_TOPIC=cyton-data
export PUBSUB_SUBSCRIPTION=cyton-data
export BIGQUERY_DATASET_ID=rtda
export BIGQUERY_TABLE_ID=cytonData

python main.py
