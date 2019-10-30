import yaml

from prefect import Flow, task
from prefect.client.secrets import Secret
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import snowflake.connector as sf


with open("query_config.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
reports = data_loaded['queries']

crons = set()
for r in reports:
    crons.add(r.get('cron_schedule', ''))


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


for cron in crons:
    schedule = Schedule(clocks=[CronClock(cron)])
    schedule.next(5)
    flow_name = 'query_executions-' + cron

    with Flow(flow_name) as flow:
        for r in reports:
            if r['cron_schedule'] == cron:
                db=r.get('database', '')
                sch=r.get('schema', '')
                q=r.get('query', '')
                ex = execute_snowflake_query(sch,db,q)

                row_count = ex[0]