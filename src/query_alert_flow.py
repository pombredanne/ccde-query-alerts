import os
import sys
import yaml

from kawasemi import Kawasemi
from prefect import Flow, task
from prefect.client import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from .custom_tasks.snowflakequery import SnowflakeExecution

SNOWFLAKE_ACCOUNT = 'jh72176.us-east-1'
SNOWFLAKE_USER = 'PREFECT_READ_ONLY'
SNOWFLAKE_ROLE = 'ANALYST_BASIC'
SNOWFLAKE_WH = 'COMPUTE_WH'


with open("src/query_config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
reports = data_loaded['queries']

crons = set()
for r in reports:
    crons.add(r.get('cron_schedule', ''))

for cron in crons:
    schedule = Schedule(clocks=[CronClock(cron)])
    schedule.next(5)
    flow_name = 'query_executions-' + cron

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

    with Flow(flow_name, schedule) as flow:
        pw = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
        for r in reports:

            if r['cron_schedule'] == cron:

                query = SnowflakeExecution(
                    SNOWFLAKE_ACCOUNT,
                    SNOWFLAKE_USER,
                    pw,
                    database=r.get('database', ''),
                    schema=r.get('schema', ''),
                    role=SNOWFLAKE_ROLE,
                    warehouse=SNOWFLAKE_WH,
                    query=r.get('query', ''))

                alert = slack_query_alert(query[0], r)




