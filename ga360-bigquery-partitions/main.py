import logging
import base64
import json
from google.cloud import bigquery
import re

# replace with your dataset
DEST_DATASET = 'REPLACE_DATASET'

def make_partition_tbl_name(table_id):
    t_split = table_id.split('_20')

    name = t_split[0]

    suffix =  ''.join(re.findall("\d\d", table_id)[0:4])
    name = name + '$' + suffix

    logging.info('partition table name: {}'.format(name))

    return name


def copy_bq(dataset_id, table_id):
    client = bigquery.Client()
    dest_dataset = DEST_DATASET
    dest_table = make_partition_tbl_name(table_id)

    source_table_ref = client.dataset(dataset_id).table(table_id)
    dest_table_ref = client.dataset(dest_dataset).table(dest_table)

    job = client.copy_table(
        source_table_ref,
        dest_table_ref,
        location = 'EU') # API request

    logging.info('Copy job: dataset {}: tableId {} -> dataset {}: tableId {} - '
                 'check BigQuery logs of job_id: {} for status'.format(
        dataset_id, table_id, dest_dataset, dest_table,
        job.job_id))

def extract_data(data):
    """Gets the tableId, datasetId from pub/sub data"""
    data = json.loads(data)
    table_info = data['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['load']['destinationTable']
    logging.info('Found data: {}'.format(json.dumps(table_info)))
    return table_info

def bq_to_bq(data, context):
    if 'data' in data:
        table_info = extract_data(base64.b64decode(data['data']).decode('utf-8'))
        copy_bq(dataset_id=table_info['datasetId'], table_id=table_info['tableId'])
    else:
        raise ValueError('No data found in pub-sub')