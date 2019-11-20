import os
from prefect import Flow, task, utilities
from .helpers.help import print_test

LOGGER = utilities.logging.configure_logging(testing=False)


@task
def print_envs():
    a = os.getenv('SFK_USER')
    LOGGER.info(a)
    print_test('blah')
    print_test('help')
    print_test('test')
    return a


with Flow('test env vars') as flow:
    test = print_envs()
    LOGGER.info(test)
