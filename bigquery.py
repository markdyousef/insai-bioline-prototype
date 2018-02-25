from google.cloud import bigquery
import os
import json

DATASET_ID = os.getenv('BIGQUERY_DATASET_ID')
TABLE_ID = os.getenv('BIGQUERY_TABLE_ID')
AC_COUNT = 3 # accel data
CH_COUNT = 8 # channel data
AX_COUNT = 6 # aux data

def get_schema():
    schema = []

    # accel schema
    ac_fields = [bigquery.SchemaField('ac{}'.format(i), 'FLOAT') for i in range(AC_COUNT)]
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
    except Exception as e:
        print(e)
    schema = get_schema()
    table = bigquery.Table(table_ref, schema)
    return client.create_table(table)

def format_data(data):
    # message data is bytes
    accel_data = data['accelData']
    channel_data = data['channelData']
    aux_data = data['auxData']['data']
    sample_number = data['sampleNumber']
    start_byte = data['startByte']
    valid = data['valid']
    timestamp = data['timestamp']
    board_time = data['boardTime']
    
    # format data in accordance to schema
    accel_data = [{'ac{}'.format(i): val} for i, val in enumerate(accel_data)]
    channel_data = [{'ch{}'.format(i): val} for i, val in enumerate(channel_data)]
    aux_data = [{'ax{}'.format(i): val} for i, val in enumerate(aux_data)]

    row = {
        "accelData": accel_data,
        "channelData": channel_data,
        "auxData": aux_data,
        "sampleNumber": sample_number,
        "startByte": start_byte,
        "valid": valid,
        "timestamp": timestamp,
        "boardTime": board_time
    }
    return row

def stream_data_bigquery(client, table, data_stream):
    def save_message(message):
        message.ack()
        data = json.loads(message.data.decode())
        row = format_data(data)
        errors = client.insert_rows(table, [row])
        assert errors == []
        return errors
    
    # list is used for testing
    if isinstance(data_stream, list):
        rows = [format_data(data) for data in data_stream]
        errors = client.insert_rows(table, rows)
        print(errors)
        assert errors == []
        return errors

    future = data_stream.open(save_message)
    
    try:
        future.result()
    except Exception as e:
        future.close()
        raise

