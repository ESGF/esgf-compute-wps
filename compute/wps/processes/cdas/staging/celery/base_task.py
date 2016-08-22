from celery import Task
from engines import engineRegistry

class StagingTask(Task):
    abstract = True
    engines = engineRegistry

    def __init__(self):
        Task.__init__(self)


