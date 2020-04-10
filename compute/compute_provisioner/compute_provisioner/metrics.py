from prometheus_client import CollectorRegistry
from prometheus_client import Histogram
from prometheus_client import Summary
from prometheus_client import Counter
from prometheus_client import Gauge

registry = CollectorRegistry()

PROVISIONER_QUEUE = Gauge('provisoner_queue_total', 'Number of jobs in queue', registry=registry)
PROVISIONER_QUEUED = Counter('provisioner_queued_total', 'Total number of jobs queued', registry=registry)
PROVISIONER_QUEUE_ERROR = Counter('provisioner_queue_error_total', 'Number of times failed to queue', registry=registry)

PROVISIONER_STATE_PROCESS_DURATION = Summary('provisioner_state_process_duration_seconds', 'Number of seconds taken to process current stateu', ['state'], registry=registry)
