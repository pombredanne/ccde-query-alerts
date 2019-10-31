import os
import sys
import yaml

from kawasemi import Kawasemi
from prefect import Flow, task
from prefect.client import Secret
import snowflake.connector as sf


@task
def get_queries():
    sys.path.append(os.getcwd())
    with open("query_config.yaml", 'r') as stream:
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
        webhook = Secret("QUERY-ALERT-SLACK-WH").get()
        slack_config = {"CHANNELS":
                            {"slack":
                                 {"_backend": "kawasemi.backends.slack.SlackChannel",
                                  "url": webhook,
                                  "username": "Snowflake Query Alert",
                                  "channel": report['channel']}
                             }
                        }
        kawasemi = Kawasemi(slack_config)
        message = 'Alert for: ' + report['query_name'] + '\n' + \
                  report['slack_message'] + '\n' + "Current row count is " + str(row_count)
        kawasemi.send(message)


with Flow('query_alerts') as flow:
    queries = get_queries()
    executions = execute_snowflake_query.map(queries)
    send_alerts = slack_query_alert.map(executions)

flow.run()