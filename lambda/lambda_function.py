import awswrangler as wr
import pandas as pd
import urllib.parse
import os

GLUE_DATABASE = os.environ['glue_catalog_db_name']
GLUE_TABLE = os.environ['glue_catalog_table_name']
WRITE_MODE = os.environ['write_data_operation']

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'],
        encoding='utf-8'
    )

    try:
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')

        df = pd.json_normalize(df_raw['items'])

        file_name = key.split('/')[-1]
        region = file_name.split('_')[0]
        df['region'] = region
      
        wr_response = wr.s3.to_parquet(
            df=df,
            database=GLUE_DATABASE,
            table=GLUE_TABLE,
            dataset=True,
            mode=WRITE_MODE
        )

        print("Write successful:", wr_response)
        return wr_response

    except Exception as e:
        print(f"Error: {e}")
        raise e
