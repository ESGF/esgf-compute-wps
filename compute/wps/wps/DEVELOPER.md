# Developer Guide

### Adding a New Task

> This will guide you in implementing a new task. A task is responsible for retrieving,
processing and preparing data. Below you can find a bare minimum example. See
[CDAT](blob/master/compute/wps/tasks/cdat.py) or [EDAS](blob/master/compute/wps/tasks/edas.py)
for sample implementations.

```python
from wps.tasks import base

@base.register_process('CDAT.example', abstract="Regrids a variable to designated grid. Required parameter named gridder.")
@base.cwt_shared_task()
def example(self, parent_variables, variables, domains, operation, user_id, job_id):
  """ Performs some operation on data.

  Args:
    parent_variables: A dict mapping names of variables to their dict
      representations. These are the variables return from processes which are
      inputs to the current process. Something you would see in a workflow.
    variables: A dict mapping names of variables to their dict representations.
    domains: A dict mapping names of domains to their dict representations.
    operation: A dict representations of the current operations parameters.

  Returns:
    A dict mapping the name of the output to a cwt.Variable instance.
  """

  v, d, o = self.load(parent_variables, variables, domains, operation)

  # Some processing of inputs

  return {o.name: cwt.Variable('http://aims2.llnl.gov/threddsCWT/', 'tas')}
```

### Adding a New Backend

> This will guide you in implementing a new backend. A backend is responsible for
registering processes and preparing celery signatures to be consumed by the
framework. See [CDAT](blob/master/compute/wps/backends/local.py) or [EDAS](blob/master/compute/wps/backends/edas.py) for sample implementations.

##### Registering a process

> Processes will be registered when populate_processes is called. Each process will
need to call add_process with a str identifier, title and abstract for the process.

##### Preparing celery signatures for execution

> When a WPS execute request is consumed by the service either execute or workflow
will be called. Each method will need to return a celery signature of the task to
be executed. See [celery documentation](http://docs.celeryproject.org/en/latest/userguide/canvas.html)
workflow guide.

```python
class CDAT(backend.Backend):
    def initialize(self):
        """ Performs some initialization for the backend.

        This will be called before populate_processes.
        """

    def populate_processes(self):
        """ Registers the process with the framework.

        For each process that will be registered call self.add_process passing
        the identifier, title and abstract.  
        """

    def execute(self, identifier, variables, domains, operations, **kwargs):
        """ Prepares a celery signature for the requested operation.

        Args:
          identifier: Process str identifier.
          variables: A dict mapping names of variables to cwt.Variable instances.
          domains: A dict mapping names of domains to cwt.Domain instances.
          operations: A dict mapping names of operations to cwt.Process instances.
          **kwargs: A dict of extra arguments.
            job: A wps.models.Job instance.
            user: A wps.models.User instance.

        Returns:
          A celery signature for the requested process.

          See Adding a New Task for the format of the signature.
        """

    def workflow(self, root_op, variables, domains, operations, **kwargs):
      """ Prepares a celery signature for the requested workflow operation.

      Args:
        root_op: A cwt.Process instance of the root operation.
        variables: A dict mapping names of variables to cwt.Variable instances.
        domains: A dict mapping names of domains to cwt.Domain instances.
        operations: A dict mapping names of operations to cwt.Process instances.
        **kwargs: A dict of extra arguments.
          job: A wps.models.Job instance.
          user: A wps.models.User instance.

      Returns:
        A celery signature for the requested process.

        See Adding a New Task for the format of the signature.
      """
```
