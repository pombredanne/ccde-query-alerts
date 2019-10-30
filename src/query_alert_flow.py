from kawasemi import Kawasemi
from prefect import Flow, task
from prefect.tasks.secrets.base import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.utilities.debug import is_serializable
from prefect.tasks.snowflake import SnowflakeQuery

# from .custom_tasks.snowflakequery import SnowflakeExecution

SNOWFLAKE_ACCOUNT = 'jh72176.us-east-1'
SNOWFLAKE_USER = 'PREFECT_READ_ONLY'
SNOWFLAKE_ROLE = 'ANALYST_BASIC'
SNOWFLAKE_WH = 'COMPUTE_WH'




@task
def slack_query_alert(row_count):
    SLACK_WEBHOOK = Secret("QUERY-ALERT-SLACK-WH").get()
    if row_count > 0:
        slack_config = {"CHANNELS":
                            {"slack":
                                 {"_backend": "kawasemi.backends.slack.SlackChannel",
                                  "url": SLACK_WEBHOOK,
                                  "username": "Snowflake Query Alert",
                                  "channel": '#slack-test'}
                             }
                        }
        kawasemi = Kawasemi(slack_config)
        message = 'do something'
        kawasemi.send(message)


with Flow('test') as flow:
    s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
    query = SnowflakeQuery(
        'jh72176.us-east-1',
        'PREFECT_READ_ONLY',
        s,
        database='analytics_dw',
        schema='partner_reports',
        role='ANALYST_BASIC',
        warehouse='COMPUTE_WH',
        query='select * from analytics_dw.partner_reports.QuinStreet_Lead_ID_Report')

    # alert = slack_query_alert(query[0])

