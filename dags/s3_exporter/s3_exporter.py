from pydoc import describe
from airflow import DAG
from aws.s3_operator import S3Operator
from datetime import datetime, timedelta

DATABASE    = 'RAW'
SCHEMA      = 'PC_DUMP'
FILE_FORMAT = 'csv'

tables_key  = {
        'games'                 : 'nba/'
        , 'team_stat'           : 'nba/'
        , 'team_roster'         : 'nba/'
        , 'player_game_stat'    : 'nba/'
        , 'TEAM_BOXSCORE'       : 'nba/'
    }

with DAG(
    dag_id                  = 's3_data_exporter'
    , start_date            = datetime(2024,1,1)
    , catchup               = False
    , schedule_interval     = '0 9 * * *'
    , default_args          = None
    , tags                  = ['aws', 's3']       
    , description           = 'Daily export of nba data to s3 bucket'
) as dag:
    
    data_export     = S3Operator(
        task_id     = 's3_data_export'
        , params    = {
            'database'      : DATABASE
            , 'schema'      : SCHEMA
            , 'file_format' : FILE_FORMAT
            , 'table_query' : tables_key
        }
    )

    data_export