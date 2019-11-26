import json
import yaml

from prefect import Flow, task, utilities
from prefect.client import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import requests
import snowflake.connector as sf


schedule = Schedule(clocks=[CronClock("30 15 * * *")])
schedule.next(5)

LOGGER = utilities.logging.configure_logging(testing=False)


@task
def get_queries():
    with open("src/helpers/query_config.yaml", 'r') as stream:
        data_loaded = yaml.safe_load(stream)
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


@task
def slack_query_alert(query_execution):
    row_count = query_execution[0]
    report = query_execution[1]
    if row_count > 0:
        webhook_url = Secret("RENEWAL-TECH-SLACK-WH").get()

        slack_data = {
            "text": 'Alert for: ' + report['query_name'] + '\n' + "Current row count is " + str(row_count)
        }

        response = requests.post(
            webhook_url, data=json.dumps(slack_data),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            LOGGER.exception(
                'Request to slack returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
            )


with Flow('query_alerts', schedule) as flow:
    queries = get_queries()
    executions = execute_snowflake_query.map(queries)
    send_alerts = slack_query_alert.map(executions)