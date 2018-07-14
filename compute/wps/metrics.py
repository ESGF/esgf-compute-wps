import logging

from django.conf import settings
from prometheus_client import REGISTRY
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client import Summary
from prometheus_client import CollectorRegistry
from prometheus_client import pushadd_to_gateway

logger = logging.getLogger('wps.metrics')

REQUESTS = Summary('cwt_wps_requests', 'WPS Requests', ['method', 'request'], registry=REGISTRY)

GC_REQ_GET = REQUESTS.labels('get', 'getcapabilities')
GC_REQ_POST = REQUESTS.labels('post', 'getcapabilities')

DP_REQ_GET = REQUESTS.labels('get', 'describeprocess')
DP_REQ_POST = REQUESTS.labels('post', 'describeprocess')

EX_REQ_GET = REQUESTS.labels('get', 'execute')
EX_REQ_POST = REQUESTS.labels('post', 'execute')

ERRORS = Counter('cwt_wps_requests_error', 'WPS Requests errors', registry=REGISTRY)
