from datetime import date, datetime, timedelta
import yaml

from kawasemi import Kawasemi
from prefect import Flow, task
from prefect.client import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import snowflake.connector as sf

#
# schedule = Schedule(clocks=[CronClock("0 13 * * *")])
# schedule.next(5)
#
# @task
# def get_queries():
#     with open("src/query_config.yaml", 'r') as stream:
#         data_loaded = yaml.safe_load(stream)
#     reports = data_loaded['queries']
#     return reports[0]
#
#
# @task
# def execute_snowflake_query(di):
#     s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
#     password = s.get()
#     connect_params = {
#         "account": 'jh72176.us-east-1',
#         "user": 'PREFECT_READ_ONLY',
#         "password": password,
#         "database": di.get('database',''),
#         "schema": di.get('schema', ''),
#         "role": 'ANALYST_BASIC',
#         "warehouse": 'COMPUTE_WH',
#     }
#     conn = sf.connect(**connect_params)
#     try:
#         with conn:
#             cursor = conn.cursor()
#             executed = cursor.execute(di.get('query', ''))
#             row_count = executed.rowcount
#         conn.close()
#         return row_count, executed
#
#     except Exception as error:
#         conn.close()
#         raise error
#
#
# @task
# def slack_query_alert(row_count, report):
#     webhook = Secret("QUERY-ALERT-SLACK-WH").get()
#     if row_count > 0:
#         slack_config = {"CHANNELS":
#                             {"slack":
#                                  {"_backend": "kawasemi.backends.slack.SlackChannel",
#                                   "url": webhook,
#                                   "username": "Snowflake Query Alert",
#                                   "channel": report['channel']}
#                              }
#                         }
#         kawasemi = Kawasemi(slack_config)
#         message = 'Alert for: ' + report['query_name'] + '\n' + \
#                   report['slack_message'] + '\n' + "Current row count is " + str(row_count)
#         kawasemi.send(message)
#
#
# with Flow('ETL', schedule) as flow:
#     queries = get_queries()
#     ex = execute_snowflake_query(queries)
#     slack_query_alert(ex[0], queries)

@task
def extract():
    return [1, 2, 3]


@task
def transform(x):
    return [i * 10 for i in x]


@task
def load(y):
    print("Received y: {}".format(y))


with Flow("ETL-test") as flow:
    e = extract()
    t = transform(e)
    l = load(t)