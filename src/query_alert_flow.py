from datetime import date, datetime, timedelta
import yaml

from kawasemi import Kawasemi
from prefect import Flow, task
from prefect.client import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import snowflake.connector as sf
import sqlalchemy
import pandas as pd


@task
def get_queries():
    with open("src/query_config.yaml", 'r') as stream:
        data_loaded = yaml.safe_load(stream)
    reports = data_loaded['queries']
    return reports[0]


@task
def getDates():
    weekMin = date.today() - timedelta(days = 7)
    compareMin = weekMin - timedelta(days = 7*4)
    return {
        'today': date.today(),
        'weekMin': weekMin,
        'compareMin': compareMin
    }


@task
def getData(dates):
    compareMin = dates['compareMin']

    user = 'PREFECT_READ_ONLY'
    s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
    password = s.get()

    conn_str_src = 'snowflake://' + user + ':' + password + '@jh72176.us-east-1/ANALYTICS_DW/ANALYSIS?warehouse=COMPUTE_WH'
    engine_src = sqlalchemy.create_engine(conn_str_src)
    with engine_src.connect() as conn, conn.begin():
        table_name = 'Current data for comparisons'
        query = """
        select t1.POLICY_TERM_ID
             , t1.POLICY_CREATED_DATE
             , t1.COVERAGE_STATE
             , t1.IS_VALID
             , t1.PROOF_OF_INSURANCE
             , t2."POL_Program"
             , t2."POL_UnderwritingTier"
        from ANALYTICS_DW.ANALYSIS.POLICY_ENDORSEMENT as t1
                 left join ANALYTICS_DW.ANALYSIS.QUOTE_POLICY_TERM_CUSTOM_FIELD_PIVOT as t2
                           on t1.POLICY_ID = t2.POLICY_ID
        where t1.ENDORSEMENT_TYPE_NAME = 'PolicyIssuance'
          and t1.renewal_count = 0
        and t1.POLICY_CREATED_DATE >= '{compareMin}'
        ;
        """.format(table_name, compareMin = compareMin.strftime('%Y-%m-%d'))
        print("[CLEARCOVER: ccds.data] Fetching table [{}]".format(table_name))
        data = pd.read_sql(query, conn)

        return data

@task
def execute_snowflake_query(di):
    s = Secret("SNOWFLAKE-READ-ONLY-USER-PW")
    password = s.get()
    connect_params = {
        "account": 'jh72176.us-east-1',
        "user": 'PREFECT_READ_ONLY',
        "password": password,
        "database": di.get('database',''),
        "schema": di.get('schema'),
        "role": 'ANALYST_BASIC',
        "warehouse": 'COMPUTE_WH',
    }
    conn = sf.connect(**connect_params)
    try:
        with conn:
            cursor = conn.cursor()
            executed = cursor.execute(di.get('query', ''))
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
    # queries = get_queries()
    # ex = execute_snowflake_query(queries)
    # slack_query_alert(ex[0], queries)
    dates = getDates()
    data = getData(dates)
    # ex = execute_snowflake_query(sch, db, q)
    # row_count = ex[0]
    # alert = slack_query_alert(ex[0], r)