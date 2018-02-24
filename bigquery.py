from google.cloud import bigquery
import os

DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
TABLE_ID = os.getenv('BIGQUERY_TABLE_ID')
AC_COUNT = 3 # accel data
CH_COUNT = 8 # channel data
AX_COUNT = 6 # aux data

def get_schema():
    schema = []

    # accel schema
    ac_fields = [bigquery.SchemaField('ac{}'.format(i), 'INTEGER') for i in range(AC_COUNT)]
    ac_schema = bigquery.SchemaField('accelData', 'RECORD', 'REPEATED', fields=tuple(ac_fields))
    schema.append(ac_schema)

    # channel schema
    ch_fields = [bigquery.SchemaField('ch{}'.format(i), 'FLOAT') for i in range(CH_COUNT)]
    ch_schema = bigquery.SchemaField('channelData', 'RECORD', 'REPEATED', fields=tuple(ch_fields))
    schema.append(ch_schema)

    # aux schema
    ax_fields = [bigquery.SchemaField('ax{}'.format(i), 'INTEGER') for i in range(AX_COUNT)]
    ax_schema = bigquery.SchemaField('auxData', 'RECORD', 'REPEATED', fields=tuple(ax_fields))
    schema.append(ax_schema)

    schema.append(bigquery.SchemaField('sampleNumber', 'INTEGER'))
    schema.append(bigquery.SchemaField('startByte', 'INTEGER'))
    schema.append(bigquery.SchemaField('valid', 'BOOLEAN'))
    schema.append(bigquery.SchemaField('timestamp', 'INTEGER'))
    schema.append(bigquery.SchemaField('boardTime', 'INTEGER'))

    return schema

def get_dataset(client, dataset_id=DATASET_ID):
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    return dataset

def get_table(client, dataset_id=DATASET_ID, table_id=TABLE_ID):
    dataset = get_dataset(client, dataset_id)
    table_ref = dataset.table(table_id)
    # get table
    try:
        table = bigquery.Table(table_ref)
        return client.get_table(table)
    except Exception, e:
        print(e)
    schema = get_schema()
    table = bigquery.Table(table_ref, schema)
    return client.create_table(table)





def stream_data_bigquery(client, table, data_stream):
    def save_message(message):
        message.ack()
        data = message.data
        errors = client.insert_rows(table, data)
        assert errors == []

    
    # future = data_stream.open(save_message)
    # future.result()
    rows = [
        {
            'accelData': [{'ac1': 0}, {'ac2': 1}, {'ac3': 2}],
            # 'channelData1': [{'channel1': -0.034343, 'channel2': 0.234324}],
            'auxData_channel': {
                'type': 'Buffer',
                'data': [0,0,0,0]
            },
            'sampleNumber': 235,
            'startByte': 160,
            'stopByte': 161,
            # 'timestamp':1519464477127,
            'valid': True,
            'id': 12
        }
    ]
    errors = client.insert_rows(table, rows)
    print(errors)
    assert errors == []