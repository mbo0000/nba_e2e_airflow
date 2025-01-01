from airflow import DAG
# from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime, duration
from datetime import timedelta

SOURCE_DATABASE     = 'STAGING'
SOURCE_SCHEMA       = 'NBA_DUMP'
TARGET_DATABASE     = 'RAW'
TARGET_SCHEMA       = 'PC_DUMP'
EXTRACTOR_CONTAINER = 'nba_extractor'

def _create_table(database, schema, table, columns, connection, **kwargs):
    '''
    create a new table in snowflake
    '''
    col_len = len(columns)-1
    query = f"create or replace transient table {database}.{schema}.{table}("
    for idx, col in enumerate(columns):
        query += f"{col}"
        if idx != col_len:
            query += ", "
    query += ");"

    connection.execute(query)

def func_update_table_schema(ti, table, **kwargs):
    '''
    get columns from source table and update target table schema if applicable
    '''
    snowf_engine = SnowflakeHook(
        'snowflake_conn'
        , database = SOURCE_DATABASE
        , schema = SOURCE_SCHEMA
    ).get_sqlalchemy_engine()

    with snowf_engine.begin() as con:
        source_query    = f"select column_name, data_type from {SOURCE_DATABASE}.information_schema.columns where table_name = '{table}' and table_schema = '{SOURCE_SCHEMA}';"
        source_columns  = con.execute(source_query).fetchall()

        if not source_columns:
            return None
        
        source_cols_type    = [' '.join([col[0], col[1]]) for col in source_columns]
        # store columns for later use in a seprate task
        ti.xcom_push(key    = 'source_cols', value = source_cols_type)
        
        target_query        = f"select column_name, data_type from {TARGET_DATABASE}.information_schema.columns where table_name = '{table}' and table_schema = '{TARGET_SCHEMA}';"
        target_res          = con.execute(target_query).fetchall()

        if not target_res or len(target_res) == 0:
            
            _create_table(
                database        = TARGET_DATABASE
                , schema        = TARGET_SCHEMA
                , table         = table
                , columns       = source_cols_type
                , connection    = con
            )

            return

        # check for new columns in the staging table
        existing_cols   = [col[0] for col in target_res]
        new_cols        = [' '.join([col[0], col[1]]) for col in source_columns if col[0] not in existing_cols]

        if not new_cols or len(new_cols) == 0:
            return

        add_col_query   = f"alter table {TARGET_DATABASE}.{TARGET_SCHEMA}.{table} add column "   
        new_cols_num    = len(new_cols)-1
        for idx,col in enumerate(new_cols):
            add_col_query += f"{col}"
            if idx != new_cols_num:
                add_col_query += ', '
        add_col_query += ';'

        con.execute(add_col_query)

def func_merge_tables(ti, table, case_field, entity_id, **kwargs):

    '''
    merging updated or new records in the source into the target table
    '''

    # get the list of columns from prev task
    source_cols = ti.xcom_pull(key = 'source_cols', task_ids = 'update_table_schema')

    if not source_cols:
        return None

    # construct merge tables query
    query = f"merge into {TARGET_DATABASE}.{TARGET_SCHEMA}.{table} as target using {SOURCE_DATABASE}.{SOURCE_SCHEMA}.{table} as source on target.{entity_id} = source.{entity_id} "\
            f"\n when matched"

    # when entity id matched but existing record is updated, then update all record's values
    if case_field:
        for k,val in case_field.items():
            query += f" and source.{k} {val} target.{k}"
    
    query += f" then update set "

    cols_len = len(source_cols) - 1

    for idx, col in enumerate(source_cols):
        col_name, _ = col.split(' ')
        query += f"target.{col_name} = source.{col_name}"
        if idx != cols_len:
            query += ", "
    
    # if new record, then appending to table
    query       += "\n when not matched then insert ("
    values_q    = ""
    for idx, col in enumerate(source_cols):
        col_name, _     = col.split(' ')
        query           += f"{col_name}"
        values_q        += f"source.{col_name}"
        if idx != cols_len:
            query       += ", "
            values_q    += ", "
        else:
            query += ") \n values ("
            values_q += ");"

    final_query = query + values_q

    # excute merge table query
    snowf_engine = SnowflakeHook(
        'snowflake_conn'
        , database  = SOURCE_DATABASE
        , schema    = SOURCE_SCHEMA
    ).get_sqlalchemy_engine()

    with snowf_engine.begin() as con:
        con.execute(final_query)

def func_check_down_stream_dag(down_stream_dag_id):
    '''
    check if there is a downstream dag id to trigger, otherwise finish the dag
    '''
    return 'trigger_downstream_dag' if down_stream_dag_id else 'dag_finished'

def func_job_clean_up(table):
    
    '''
    remove source table once task completed
    '''

    snowf_engine = SnowflakeHook(
        'snowflake_conn'
        , database  = SOURCE_DATABASE
        , schema    = SOURCE_SCHEMA
    ).get_sqlalchemy_engine()

    query = f"drop table if exists {table};"
    with snowf_engine.begin() as con:
        con.execute(query)

def generate_dag(dag_id, entity, case_field, entity_id, trigger_dag_id, schedule):
    dag = DAG(
        dag_id                      = dag_id
        , catchup                   = False
        , start_date                = datetime(2024,1,1)
        , schedule_interval         = schedule
        , default_args              = {
                                        "retries" : 3,
                                        "retry_delay" : duration(seconds=2),
                                        "max_retry_delay" : duration(hours=2),
                                    }
        , description               = 'Standard NBA endpoints DAG factory'
        , tags                      = ['raw', 'nba']
    )

    with dag:

        # Extract and Load to staging area
        extract_load = BashOperator(
            task_id             = 'extract_load'
            , execution_timeout =timedelta(seconds=3600)
            , bash_command      = f'docker exec {EXTRACTOR_CONTAINER} python main.py '\
                                f' --entity {entity}'\
                                f' --database {SOURCE_DATABASE}'\
                                f' --schema {SOURCE_SCHEMA}'\
                                f' --table {entity}'
        )

        # Update prod table schema if needed
        update_table_schema = PythonOperator(
            task_id             = 'update_table_schema'
            , python_callable   = func_update_table_schema
            , op_kwargs         = {'table' : entity.upper()}
        )

        # Merge update and new data from source into prod table
        merge_tables = PythonOperator(
            task_id             = 'merge_tables'
            , python_callable   = func_merge_tables
            , op_kwargs         = {
                                    'entity'        : entity
                                    , 'table'       : entity.upper()
                                    , 'case_field'  : case_field
                                    , 'entity_id'   : entity_id
                                }
        )

        # drop source table
        clean_up =  PythonOperator(
            task_id             = 'clean_up'
            , python_callable   = func_job_clean_up
            , op_kwargs         = {'table' : entity.upper()}
        )

        # check for down stream dag
        check_dag_downstream = BranchPythonOperator(
            task_id = 'check_dag_downstream'
            , python_callable = func_check_down_stream_dag
            , op_kwargs = {'down_stream_dag_id' : trigger_dag_id}
        )

        # trigger downstream/ext dag
        trigger_downstream_dag = TriggerDagRunOperator(
            task_id = 'trigger_downstream_dag'
            , trigger_dag_id = f"nba_package_{trigger_dag_id}"
            # passing values to downstream dag if needed
            , conf = {'message':''}
        )

        # finishing task
        dag_finished = BashOperator(
            task_id = 'dag_finished'
            , bash_command = f'echo "DAG {dag_id} is completed."'
        )

        extract_load >> update_table_schema >> merge_tables >> clean_up >> check_dag_downstream >> [trigger_downstream_dag, dag_finished]



# entities configs
ENTITIES = [
    {
        'games': {
            'id'                : 'game_id'
            , 'case_field'      : {'WL':'<>', 'TEAM_ID': '='}
            , 'trigger_dag_id'  : 'team_boxscore'
            , 'schedule'        : '0 9 * * *'
        }
    }
    # only run once
    # , {
    #     'teams': {
    #         'id'                : 'id'
    #         , 'case_field'      : {'full_name':'<>'}
    #         , 'trigger_dag_id'  : ''
    #         , 'schedule'        : '@daily'
    #     }
    # }
    , {
        'team_stat' :  {
            'id'                : 'id'
            , 'case_field'      : {'year':'=', 'gp':'<>'}
            , 'trigger_dag_id'  : ''
            , 'schedule'        : '0 9 * * *'
        }
    }
    , {
        'team_roster' :  {
            'id'                : 'id'
            , 'case_field'      : {'teamid': '<>'}
            , 'trigger_dag_id'  : ''
            , 'schedule'        : '0 8 1 * *'
        }
    }
    , {
        'player_game_stat' :  {
            'id'                : 'id'
            , 'case_field'      : {}
            , 'trigger_dag_id'  : ''
            , 'schedule'        : '0 9 * * *'
        }
    }

    , {
        'team_boxscore' :  {
            'id'                : 'gameid'
            , 'case_field'      : {'teamid':'='}
            , 'trigger_dag_id'  : ''
            , 'schedule'        : None
        }
    }
]

for ent in ENTITIES:
    for k,val in ent.items():
        dag_id              = f"nba_package_{k}"
        globals()[dag_id]   = generate_dag(
            dag_id
            , k
            , val['case_field']
            , val['id']
            , val['trigger_dag_id']
            , val['schedule']
        )
        