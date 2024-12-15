from airflow.models import BaseOperator
from gsheet_hook import GSheetsHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import time

class GSheetDumpOperator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs) 
        
        self.gsheet_id      = kwargs['params']['gsheet_id']
        self.tables         = kwargs['params']['tables']
        self.database       = kwargs['params']['database']
        self.schema         = kwargs['params']['schema']

    def execute(self, context):

        snowf_engine = SnowflakeHook(
            'snowflake_conn'
            , database  = self.database
            , schema    = self.schema
        ).get_sqlalchemy_engine()

        hook = GSheetsHook()

        with snowf_engine.begin() as conn:
            for table in self.tables:
                query   = f'select * from {self.database}.{self.schema}.{table};'
                df      = pd.read_sql_query(query, conn)
                df      = df.astype(str)
                hook.update_worksheet(self.gsheet_id, df, table)
                time.sleep(1)

        