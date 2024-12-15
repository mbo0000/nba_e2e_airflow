from airflow import DAG
from datetime import datetime
from google.gsheet_dump_operator import GSheetDumpOperator
import os

DATABASE    = 'CLEAN'
SCHEMA      = 'NBA'

with DAG(
    dag_id                  = 'nba_gsheet_dump'
    , start_date            = datetime(2024,1,1)
    , catchup               = False
    , schedule_interval     = '0 9 * * *'
    , default_args          = None
    , tags                  = ['gc', 'sheet']       
    , description           = 'Snowflake data dump to Google Sheet'
) as dag:
    
    gsheet_data_dump    = GSheetDumpOperator(
        task_id         = 'gsheet_dump'
        , params        = {
            'gsheet_id'     : os.getenv('GSHEET_NBA_DUMP')
            , 'tables'      : ['GAMES']
            , 'database'    : DATABASE
            , 'schema'      : SCHEMA
        }
    )

    gsheet_data_dump