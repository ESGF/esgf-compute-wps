# Developer Guide
This guide will cover how to add a new operation. Please remember to review the [CONTRIBUTING](CONTRIBUTING.md) guide.

## Defining the operation
First you'll need to define the function, register it as a Celery task and lastly register the operation with the service.

```python
from compute_tasks import base

@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, inputs=1)
@base.cwt_shared_task()
def subset_func(self, context):
  return context
```

### Define the function
```python
def subset_func(self, context):
  return context
```
The function is defined with two arguments; the first `self` is required since the function is a Celery task and will reference
a global task instance. The second; `context` is an instance of [OperationContext](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/context.py#L439).
The context is used to store details about the job, metrics and the outputs of the operation. The function must return the context instance. 

### Register Celery task
```python
@base.cwt_shared_task()
```
This decorator registers the function with the Celery subsystem.

### Register operation with the service
```python
@base.register_process('CDAT', 'subset', abstract=SUBSET_ABSTRACT, inputs=1)
```
This decorator registers the function with the operation service which when spun up will register it as a WPS operation. 
See the [documentation](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/base.py#L83)
for details on the arguments.

## Implementing the operation
The `context` argument stores the three inputs to the WPS operation; variable, domain and operation. The operation can be implemented
however it is desired. There are some supplied classes and functions to assist.

### CDAT classes and functions
The main function is [`process`](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/cdat.py#L215).
```python
def process(context, process_func=None, aggregate=False):
```
This function takes a `OperationContext` as `context`, a function as `process_func` that will be describe below 
and a bool `aggregate` that will cause the inputs to be treated as an aggregation.
```python
def process_dask(operation, *inputs):
```
The above snippet is an example of the function signature for the `process_func` argument. You can choose to implement your own or use
the already existing [`process_input`](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/cdat.py#L507)
function. This function is used with the `partial` function to customize it, see these [examples](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/cdat.py#L640-L647).
If you want this operation to be available for use in workflows then you'll need to register the `process_func` with [`PROCESS_FUNC_MAP`](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/cdat.py#L640-L647).
Additionally if you use `process_input` and the ufunc requires stacking of the input arrays you'll need to register the ufunc's `__name__`
attribute with [`REQUIRES_STACK`](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/cdat.py#L634-L638).

### Setting the output
To set the outputs of the operation you'll need to append them to the `output` attribute of `context`. There are two supported types
for these values; `str` or `cwt.Variable`. If the output is a str value then appending the `output` attribute manually is fine but if
the output is a file that's to be served by the THREDDS server the it's preferred to use the [`build_output_variable`](https://github.com/ESGF/esgf-compute-wps/blob/4b207de4fd37a29778e1c58b3c0482b2f8879b0e/compute/compute_tasks/compute_tasks/context.py#L504-L511)
method of `context`. This method will build and return a local path to write data, an append a `cwt.Variable` to the `output` attribute
automatically.
