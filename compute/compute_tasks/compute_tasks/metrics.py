import os
import logging
from urllib import error

from prometheus_client import push_to_gateway
from prometheus_client import CollectorRegistry
from prometheus_client import Gauge
from prometheus_client import Counter
from prometheus_client import Histogram
from prometheus_client import Summary

logger = logging.getLogger(__name__)

PROMETHEUS_PUSHGATEWAY_HOST = os.environ.get('PROMETHEUS_PUSHGATEWAY_HOST', '127.0.0.1')

task_registry = CollectorRegistry()

TASK_BUILD_WORKFLOW_DURATION = Summary('task_build_workflow_duration_seconds', 'Time to build a workflow', registry=task_registry)

TASK_EXECUTE_WORKFLOW_DURATION = Summary('task_execute_workflow_duration_seconds', 'Time to execute a workflow', registry=task_registry)

TASK_WORKFLOW_SUCCESS = Counter('task_workflow_success_total', 'Number of successfuly workflows', registry=task_registry)
TASK_WORKFLOW_FAILURE = Counter('task_workflow_failure_total', 'Number of failed workflows', registry=task_registry)

TASK_PREPROCESS_BYTES = Summary('task_preprocess_bytes', 'Number of bytes before processing', ['identifier'], registry=task_registry)
TASK_POSTPROCESS_BYTES = Summary('task_postprocess_bytes', 'Number of bytes post processing', ['identifier'], registry=task_registry)

TASK_PROCESS_USED = Counter('task_process_used_total', 'Number of times a process has been used', ['identifier'], registry=task_registry)

TASK_DATA_ACCESS = Counter('task_data_access_total', 'Number of times data is requested from a host', ['host'], registry=task_registry)
TASK_DATA_ACCESS_FAILURE = Counter('task_data_access_failure_total', 'Number of times data access fails', ['host'], registry=task_registry)

def push(job):
    try:
        push_to_gateway(PROMETHEUS_PUSHGATEWAY_HOST, job=job, registry=task_registry)
    except error.URLError:
        logger.error(f'Failed to push metrics to {PROMETHEUS_PUSHGATEWAY_HOST}')

backend_registry = CollectorRegistry()

BACKEND_STATE_PROCESS_DURATION = Summary('backend_state_process_duration_seconds', 'Number of seconds process current state transition', ['state'], registry=backend_registry)

BACKEND_MISSED_HEARTBEAT = Gauge('backend_missed_heartbeat_total', 'Number of times heartbeat was missed', registry=backend_registry)

BACKEND_PROVISIONER_RECONNECT = Counter('backend_provisioner_reconnect_total', 'Number of times backend reconnected to provisioner', registry=backend_registry)

BACKEND_RESOURCE_RESPONSE = Counter('backend_resource_response_total', 'Number of resource request responses', ['status'], registry=backend_registry)

BACKEND_BUILD_WORKFLOW = Summary('backend_build_workflow_duration_seconds', 'Number of seconds to build workflow', registry=backend_registry)
