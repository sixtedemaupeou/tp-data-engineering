import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
from pymongo import MongoClient


def filter_datasets(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to keep only datasets where the terms mobilité or transport are present."""
    return df[df.title.str.lower().str.contains('mobilité|transport', regex=True)
                | df.tags.str.lower().str.contains('mobilité|transport', regex=True)
                | df.description.str.lower().str.contains('mobilité|transport', regex=True)]


def get_s3_resource(minio_container_ip: str):
    """Utility function to get an s3 resource from Minio"""
    return boto3.resource('s3', 
        endpoint_url=f'http://{minio_container_ip}:9000', 
        config=boto3.session.Config(signature_version='s3v4'),
        aws_access_key_id='minioadmin',
        aws_secret_access_key='miniopassword'
    )

def get_boto3_client(minio_container_ip: str):
    """Utility function to get an s3 client connecting to Minio"""
    return boto3.client(
        's3',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='miniopassword',
        endpoint_url=f'http://{minio_container_ip}:9000'
    )

def store_datasets(datasets_df: pd.DataFrame, minio_container_ip:str, bucket_name: str = 'datagouv') -> None:
    """Store datasets in a Minio bucket in an object named with the current date (YYYY-MM-DD)"""
    print(minio_container_ip)
    s3 = get_s3_resource(minio_container_ip)
    csv_buffer = StringIO()
    datasets_df.to_csv(csv_buffer)
    current_date = datetime.today().strftime('%Y-%m-%d')
    if bucket_name not in [bucket.name for bucket in s3.buckets.all()]:
        s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f'{current_date}/df.csv').put(Body=csv_buffer.getvalue())


def load_datasets(minio_container_ip: str, bucket_name = 'datagouv') -> pd.DataFrame:
    s3_client = get_boto3_client(minio_container_ip)
    # TODO: Make current date a param of the DAG
    current_date = datetime.today().strftime('%Y-%m-%d')
    obj = s3_client.get_object(Bucket=bucket_name, Key=f'{current_date}/df.csv')
    return pd.read_csv(obj['Body'])



def fetch_filter_store_datasets(**kwargs) -> None:
    stable_datasets_url = 'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3'
    datasets_df = pd.read_csv(stable_datasets_url, sep=';')
    print(len(datasets_df))
    filtered_datasets_df = filter_datasets(datasets_df)
    print(len(filtered_datasets_df))
    store_datasets(filtered_datasets_df, kwargs['minio_container_ip'])


def get_top_orgs(n_orgs: int=30) -> pd.DataFrame:
    orgs_csv_url = 'https://www.data.gouv.fr/fr/datasets/r/b7bbfedc-2448-4135-a6c7-104548d396e7'
    orgs_df = pd.read_csv(orgs_csv_url, sep=';')
    top_orgs_df = orgs_df.sort_values(by='metric.followers', ascending=False)[:n_orgs]
    return top_orgs_df


def create_mongo_document(datasets_df: pd.DataFrame, mongo_container_ip: str) -> None:
    client = MongoClient(mongo_container_ip, 27017)
    tops_collection = client.datagouv.tops
    tops_collection.insert_one({"top_30_orgs_datasets": datasets_df.to_dict('records')})


def extract_top_datasets(**kwargs) -> None:
    """Fetch datasets from S3, filter to keep only top orgs and store in Mongo"""
    datasets_df = load_datasets(kwargs['minio_container_ip'])
    top_orgs_df = get_top_orgs()

    filtered_datasets_df = pd.merge(datasets_df, top_orgs_df, left_on="organization_id", right_on="id", how='inner')
    filtered_datasets_df = filtered_datasets_df[['id_x', 'title', 'metric.followers_x', 'organization_id', 'metric.followers_y']]
    filtered_datasets_df = filtered_datasets_df.rename(columns={"id_x": "id", "metric.followers_x": "metric.followers", "metric.followers_y": "metric.organization_followers"})

    create_mongo_document(filtered_datasets_df, kwargs['mongo_container_ip'])