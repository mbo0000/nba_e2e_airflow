import boto3
from airflow.hooks.base import BaseHook
import logging
from botocore.exceptions import NoCredentialsError, ClientError
import os

class S3Hook(BaseHook):

    def __init__(self):
        self.bucket_name    = os.getenv('AWS_S3_BUCKET_NBA')
        self.access_key     = os.getenv('AWS_ACCESS_KEY')
        self.secret         = os.getenv('AWS_SECRET_KEY')

        if not self.access_key or not self.secret:
            logging.error('Missing AWS Credentials')

        self.client = boto3.client(
            's3',
            region_name             = 'us-west-1',
            aws_access_key_id       = self.access_key,
            aws_secret_access_key   = self.secret
        )

    def upload(self, local_file, s3_file_key):

        if not self._bucket_exist(self.bucket_name):
            return None

        try:
            logging.info(f'Uploading file to {self.bucket_name} is starting.')
            self.client.upload_file(
                Filename    = local_file
                , Bucket    = self.bucket_name
                , Key       = s3_file_key
            )
            logging.info(f'Uploading file to {self.bucket_name} completed.')

            self.client.close()
            
        except ClientError as e:
            logging.error(e)    
    
    def _bucket_exist(self, bucket=None):
        if not bucket:
            bucket = self.bucket_name
            logging.info(f'Using default bucket: {bucket}')

        logging.info(f'Checking for bucket: {bucket}')
        res     = self.client.list_buckets()['Buckets']
        buckets = [b['Name'] for b in res]
        if bucket in buckets:
            logging.info(f"Bucket {bucket} found.")
            return True
        
        logging.error(f'Bucket {bucket} does not exist.')
        return False