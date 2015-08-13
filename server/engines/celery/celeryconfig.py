from kombu import Queue
## Broker settings.
BROKER_URL = 'amqp://guest@localhost//'

# List of modules to import when celery starts.
CELERY_IMPORTS = ( 'engines.celery.tasks', )

## Using the database to store task state and results.
CELERY_RESULT_BACKEND = 'amqp'

CELERYD_PREFETCH_MULTIPLIER =  4

CELERY_WORKER_DIRECT = 1

# CELERY_ANNOTATIONS = {'tasks.add': {'rate_limit': '10/s'}}
