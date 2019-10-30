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

reports = [{'query_name': 'first_query', 'database': 'analytics_dw', 'schema': 'analysis', 'query': 'select * from analytics_dw.partner_reports.QuinStreet_Lead_ID_Report', 'channel': '#slack-test', 'slack_message': 'help! we need to look at the query', 'cron_schedule': '0 12 * * *'}, {'query_name': 'second_query', 'database': 'analytics_dw', 'schema': 'analysis', 'query': "select * from clearcover_dw.acquisition_public.renewal_notices rn left join clearcover_dw.acquisition_public.renewal_notice_decisions rnd ON rnd.renewal_notice_id = rn.id where rnd.id is NULL and rn.open = 'f'", 'channel': '#slack-test', 'slack_message': 'help! we need to look at the query', 'cron_schedule': '0 13 * * *'}, {'query_name': 'third_query', 'database': 'analytics_dw', 'schema': 'analysis', 'query': 'select * from analytics_dw.partner_reports.QuinStreet_Lead_ID_Report', 'channel': '#slack-test', 'slack_message': 'help! we need to look at the query', 'cron_schedule': '0 13 * * *'}, {'query_name': 'fourth_query', 'database': 'analytics_dw', 'schema': 'analysis', 'query': 'select * from analytics_dw.partner_reports.QuinStreet_Lead_ID_Report', 'channel': '#slack-test', 'slack_message': 'help! we need to look at the query', 'cron_schedule': '0 13 * * *'}]


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


with Flow('test') as flow:
    pw = Secret("SNOWFLAKE-READ-ONLY-USER-PW").get()
    for r in reports:

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

flow.run()

