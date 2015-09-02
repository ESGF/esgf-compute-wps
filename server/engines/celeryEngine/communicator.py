from engines.communicator import ComputeEngineCommunicator, TaskMonitor
from tasks import execute

class CeleryTaskMonitor(TaskMonitor):

    def __init__( self, id, **args ):
        TaskMonitor. __init__( self, id, **args )
        self.task = args.get('task',None)

    def status(self):
        return self.task.status

    def ready(self):
        return self.task.ready()

    def result(self):
        return self.task.get()

    def taskName(self):
        return self.task.task_name

class CeleryCommunicator( ComputeEngineCommunicator ):

    def __init__( self ):
        ComputeEngineCommunicator.__init__( self )


    def submitTask( self, task_request, worker ):
        task = execute.apply_async( (task_request.task,), exchange='C.dq', routing_key=worker )
        return CeleryTaskMonitor( task.id, task=task )

    def getWorkerStats(self):
        import celery
        return celery.current_app.control.inspect().stats()
