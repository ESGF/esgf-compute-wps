import os
import logging
from urllib import error

from prometheus_client import push_to_gateway
from prometheus_client import CollectorRegistry
from prometheus_client import Counter
from prometheus_client import Histogram
from prometheus_client import Summary

logger = logging.getLogger(__name__)

PROMETHEUS_PUSHGATEWAY_HOST = os.environ.get('PROMETHEUS_PUSHGATEWAY_HOST', '127.0.0.1')

task_registry = CollectorRegistry()

TASK_BUILD_WORKFLOW_DURATION = Summary('task_build_workflow_duration_seconds', 'Time to build a workflow')

TASK_EXECUTE_WORKFLOW_DURATION = Summary('task_execute_workflow_duration_seconds', 'Time to execute a workflow')

TASK_WORKFLOW_SUCCESS = Counter('task_workflow_success_total', 'Number of successfuly workflows')
TASK_WORKFLOW_FAILURE = Counter('task_workflow_failure_total', 'Number of failed workflows')

TASK_PREPROCESS_BYTES = Summary('task_preprocess_bytes', 'Number of bytes before processing', ['identifier'])
TASK_POSTPROCESS_BYTES = Summary('task_postprocess_bytes', 'Number of bytes post processing', ['identifier'])

TASK_PROCESS_USED = Counter('task_process_used_total', 'Number of times a process has been used', ['identifier'])

TASK_DATA_ACCESS = Counter('task_data_access_total', 'Number of times data is requested from a host', ['host'])
TASK_DATA_ACCESS_FAILURE = Counter('task_data_access_failure_total', 'Number of times data access fails', ['host'])

def push(job):
    try:
        push_to_gateway(PROMETHEUS_PUSHGATEWAY_HOST, job=job, registry=task_registry)
    except error.URLError:
        logger.error(f'Failed to push metrics to {PROMETHEUS_PUSHGATEWAY_HOST}')
