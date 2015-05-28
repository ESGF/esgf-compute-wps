import logging
logger = logging.getLogger('celery.task')
from celery import Task
from engines.domain import domainRegistry

class DomainBasedTask(Task):
    abstract = True
    domains = domainRegistry

    def __init__(self):
        Task.__init__(self)


