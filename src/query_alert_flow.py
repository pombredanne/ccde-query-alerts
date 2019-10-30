import yaml

from kawasemi import Kawasemi
from prefect import Flow, task
from prefect.client.secrets import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import snowflake.connector as sf

with open("src/query_config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
reports = data_loaded['queries']


@task
def execute_snowflake_query(schema, database, query):
    s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
    SNOWFLAKE_PW = s.get()
    connect_params = {
        "account": 'jh72176.us-east-1',
        "user": 'PREFECT_READ_ONLY',
        "password": SNOWFLAKE_PW,
        "database": database,
        "schema": schema,
        "role": 'ANALYST_BASIC',
        "warehouse": 'COMPUTE_WH',
    }
    conn = sf.connect(**connect_params)
    try:
        with conn:
            cursor = conn.cursor()
            executed = cursor.execute(query)
            row_count = executed.rowcount
        conn.close()
        return row_count, executed

    except Exception as error:
        conn.close()
        raise error


@task
def slack_query_alert(row_count, report):
    SLACK_WEBHOOK = Secret("QUERY-ALERT-SLACK-WH").get()
    if row_count > 0:
        slack_config = {"CHANNELS":
                            {"slack":
                                 {"_backend": "kawasemi.backends.slack.SlackChannel",
                                  "url": SLACK_WEBHOOK,
                                  "username": "Snowflake Query Alert",
                                  "channel": r['channel']}
                             }
                        }
        kawasemi = Kawasemi(slack_config)
        message = 'Alert for: ' + report['query_name'] + '\n' + \
                  report['slack_message'] + '\n' + "Current row count is " + str(row_count)
        kawasemi.send(message)


with Flow('query_alerts') as flow:
    for r in reports:
        db = r.get('database', '')
        sch = r.get('schema', '')
        q = r.get('query', '')
        ex = execute_snowflake_query(sch, db, q)
        row_count = ex[0]
        slack_query_alert(row_count, r)
