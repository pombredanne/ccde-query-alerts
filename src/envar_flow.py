import os
from prefect import Flow, task, utilities


LOGGER = utilities.logging.configure_logging(testing=False)


@task
def print_envs():
    a = os.getenv('SFK_USER')
    LOGGER.info(a)
    b = os.getenv('ENV')
    LOGGER.info(b)
    c = os.getenv('WH')
    LOGGER.info(c)
    return a, b, c


with Flow('test env vars') as flow:
    test = print_envs()
    LOGGER.info(test)