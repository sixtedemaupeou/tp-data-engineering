import boto3
from datetime import datetime
import pandas as pd
from io import StringIO
import requests


def filter_datasets(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to keep only datasets where the terms mobilité or transport are present."""
    return df[df.title.str.lower().contains('mobilité|transport', regex=True)
                | df.tags.str.lower().contains('mobilité|transport', regex=True)
                | df.description.str.lower().contains('mobilité|transport', regex=True)]


def store_datasets(datasets_df: pd.DataFrame, bucket = 'datagouv') -> None:
    """Store datasets in a Minio bucket in an object named with the current date (YYYY-MM-DD)"""
    s3 = boto3.resource('s3', 
        endpoint_url='http://0.0.0.0:9000', 
        config=boto3.session.Config(signature_version='s3v4')
    )
    csv_buffer = StringIO()
    datasets_df.to_csv(csv_buffer)
    current_date = datetime.today().strftime('%Y-%m-%d')
    s3.Object(bucket, current_date).put(Body=csv_buffer.getvalue())



def fetch_filter_store_datasets() -> None:
    stable_datasets_url = "https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3"
    datasets_df = pd.read_csv(stable_datasets_url, sep=";")
    filtered_datasets_df = filter_datasets(datasets_df)
    store_datasets(filtered_datasets_df)