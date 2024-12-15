from s3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import os
import logging

class S3Operator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs) 

        # extract and set params from kwargs
        self.database       = kwargs['params']['database']
        self.schema         = kwargs['params']['schema']
        self.file_format    = kwargs['params']['file_format']
        self.table_query    = kwargs['params']['table_query']

    def execute(self, context):
        
        snowf_engine = SnowflakeHook(
            'snowflake_conn'
            , database  = self.database
            , schema    = self.schema
        ).get_sqlalchemy_engine()

        logging.info('Download data from Snowf table')

        files = {}
        with snowf_engine.begin() as conn:
            for table,key in self.table_query.items():
                query   = f'select * from {table};'
                
                # write data to file
                df          = pd.read_sql_query(query, conn)
                file        = f'{table}.{self.file_format}'
                files[file] = key + file
                df.to_csv(f'/tmp/{file}', index=False)
        
        logging.info('Uploading data to S3')
        s3_hook = S3Hook()
        for file, key in files.items():
            s3_hook.upload(f'/tmp/{file}', key)
            logging.info(f'S3 Upload: {key}')
            # remove file
            os.remove(f'/tmp/{file}')
        
        logging.info('Task completed')