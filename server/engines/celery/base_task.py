import logging
logger = logging.getLogger('celery.task')
from celery import Task

class DomainBasedTask(Task):
    abstract = True

    def __init__(self):
        Task.__init__(self)


