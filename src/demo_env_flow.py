import yaml

from prefect import Flow, task, config
from prefect.client import Secret
import snowflake.connector as sf


@task
def get_queries():
    with open("helpers/query_config.yaml", 'r') as stream:
        data_loaded = yaml.safe_load(stream)
    reports = data_loaded['queries']
    return reports


@task
def execute_snowflake_query(report):
    s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
    password = s.get()
    connect_params = {
        "account": 'jh72176.us-east-1',
        "user": config.dex_config.sfk_user,
        "password": password,
        "database": report.get('database', ''),
        "schema": report.get('schema', ''),
        "role": 'ANALYST_BASIC',
        "warehouse": 'COMPUTE_WH',
    }
    conn = sf.connect(**connect_params)
    try:
        with conn:
            cursor = conn.cursor()
            executed = cursor.execute(report.get('query', ''))
            row_count = executed.rowcount
        conn.close()
        return row_count, report

    except Exception as error:
        conn.close()
        raise error


with Flow('query_alerts') as flow:
    queries = get_queries()
    executions = execute_snowflake_query.map(queries)
