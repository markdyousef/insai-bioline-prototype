import unittest
from bigquery import get_dataset, get_schema, get_table, stream_data_bigquery
from google.cloud import bigquery
from google.cloud import pubsub_v1
import os
import json

# TODO: change to test environment 
DATASET_ID = 'test_rtda'
TABLE_ID = 'test_cytonData'
bq_client = bigquery.Client()

def get_messages():
    data = list(json.load(open('./sample_data.json')))
    # Message = pubsub_v1.subscriber.message.Message
    # messages = [Message(message['sampleNumber']) for message in data]
    return data

class BigQueryTest(unittest.TestCase):
    def test_get_schema(self):
        schema = get_schema()
        self.assertIsNotNone(schema)

    def test_get_dataset(self):
        dataset = get_dataset(bq_client, DATASET_ID)
        self.assertEqual(dataset.project, 'knowledge-prototype')
        self.assertEqual(dataset.dataset_id, DATASET_ID)
    
    def test_get_table(self):
        table = get_table(
            bq_client,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID)
        self.assertEqual(table.project, 'knowledge-prototype')
        self.assertEqual(table.dataset_id, DATASET_ID)
        self.assertEqual(table.table_id, TABLE_ID)
    
    def test_stream_data_bigquery(self):
        data_steam = get_messages()
        table = get_table(
            bq_client,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID)
        response = stream_data_bigquery(bq_client, table, data_steam)
        print(response)
        self.assertEqual(response, [])

if __name__ == '__main__':
    unittest.main()
        