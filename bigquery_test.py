import unittest
from bigquery import get_dataset, get_schema, get_table
from google.cloud import bigquery
import os

# TODO: change to test environment 
DATASET_ID = 'test_rtda'
TABLE_ID = 'test_cytonData'
bq_client = bigquery.Client()

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

if __name__ == '__main__':
    unittest.main()
        