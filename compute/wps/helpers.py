#! /usr/bin/env python

import json
import logging
import datetime

import cwt
import numpy as np

from wps import WPSError

logger = logging.getLogger('wps.helpers')

EDASK_QUEUE = {
    'queue': 'edask',
    'exchange': 'edask',
    'routing_key': 'edask',
}

# Set INGRESS_QUEUE to prevent breaking old code
INGRESS_QUEUE = DEFAULT_QUEUE = {
    'queue': 'ingress',
    'exchange': 'ingress',
    'routing_key': 'ingress',
}

def int_or_float(value):
    if isinstance(value, (int, float)):
        return value

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        raise WPSError('Failed to parse "{value}" as a float or int', value=value)

def determine_queue(process, estimate_size):
    try:
        estimate_time = estimate_size / process.process_rate
    except ZeroDivisionError:
        estimate_time = 0.0

    data = np.array([x.elapsed for x in process.timing_set.all()])

    if data.size == 0:
        percentile = estimate_time
    else:
        percentile = np.percentile(data, 75)

    logger.info('Estimated size %s MB at %s MB/sec will take %s seconds', estimate_size, process.process_rate, estimate_time)

    if estimate_time <= percentile:
        return DEFAULT_QUEUE
    else:
        return {
            'queue': 'priority.low',
            'exchange': 'priority',
            'routing_key': 'low',
        }

encoder = lambda x: json.dumps(x, default=json_dumps_default)
decoder = lambda x: json.loads(x, object_hook=json_loads_object_hook)

def json_dumps_default(x):
    if isinstance(x, slice):
        data = {
            '__type': 'slice',
            'start': x.start,
            'stop': x.stop,
            'step': x.step,
        }
    elif isinstance(x, cwt.Variable):
        data = {
            '__type': 'variable',
            'data': x.parameterize(),
        }
    elif isinstance(x, cwt.Domain):
        data = {
            '__type': 'domain',
            'data': x.parameterize(),
        }
    elif isinstance(x, cwt.Process):
        data = {
            '__type': 'process',
            'data': x.parameterize(),
        }
    elif isinstance(x, datetime.timedelta):
        data = {
            'data': {
                'days': x.days,
                'seconds': x.seconds,
                'microseconds': x.microseconds,
            },
            '__type': 'timedelta',
        }
    else:
        raise TypeError(type(x))

    return data

def json_loads_object_hook(x):
    if '__type' not in x:
        return x

    if x['__type'] == 'slice':
        data = slice(x['start'], x['stop'], x['step'])
    elif x['__type'] == 'variable':
        data = cwt.Variable.from_dict(x['data'])
    elif x['__type'] == 'domain':
        data = cwt.Domain.from_dict(x['data'])
    elif x['__type'] == 'process':
        data = cwt.Process.from_dict(x['data'])
    elif x['__type'] == 'timedelta':
        kwargs = {
            'days': x['data']['days'],
            'seconds': x['data']['seconds'],
            'microseconds': x['data']['microseconds'],
        }

        data = datetime.timedelta(**kwargs)

    return data
