import yaml

from kawasemi import Kawasemi
from prefect import Flow, task
from prefect.client.secrets import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from .custom_tasks.snowflakequery import SnowflakeExecution

SNOWFLAKE_ACCOUNT = 'jh72176.us-east-1'
SNOWFLAKE_USER = 'PREFECT_READ_ONLY'
s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
SNOWFLAKE_PW = s.get()
SNOWFLAKE_ROLE = 'ANALYST_BASIC'
SNOWFLAKE_WH = 'COMPUTE_WH'
SLACK_WEBHOOK = Secret("QUERY-ALERT-SLACK-WH").get()

with open("query_config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
reports = data_loaded['queries']

crons = set()
for r in reports:
    crons.add(r.get('cron_schedule', ''))

for cron in crons:
    schedule = Schedule(clocks=[CronClock(cron)])
    schedule.next(5)
    flow_name = 'query_executions-' + cron

    with Flow(flow_name) as flow:
        for r in reports:
            if r['cron_schedule'] == cron:
                query = SnowflakeExecution(
                    SNOWFLAKE_ACCOUNT,
                    SNOWFLAKE_USER,
                    SNOWFLAKE_PW,
                    database=r.get('database', ''),
                    schema=r.get('schema', ''),
                    role=SNOWFLAKE_ROLE,
                    warehouse=SNOWFLAKE_WH,
                    query=r.get('query', ''))

                row_count = query[0]
