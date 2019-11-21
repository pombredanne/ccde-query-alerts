import yaml

from prefect import Flow, task, utilities, config
from prefect.client import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import snowflake.connector as sf


schedule = Schedule(clocks=[CronClock("30 15 * * *")])
schedule.next(5)

LOGGER = utilities.logging.configure_logging(testing=False)


@task
def get_queries():
    with open("helpers/query_config.yaml", 'r') as stream:
        data_loaded = yaml.safe_load(stream)
    LOGGER.info(config.test_envar)
    reports = data_loaded['queries']
    return reports


@task
def execute_snowflake_query(report):
    s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
    password = s.get()
    connect_params = {
        "account": 'jh72176.us-east-1',
        "user": 'PREFECT_READ_ONLY',
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


with Flow('query_alerts_test') as flow:
    queries = get_queries()
    executions = execute_snowflake_query.map(queries)