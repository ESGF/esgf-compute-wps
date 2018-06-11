#! /usr/bin/env python

import logging

import numpy as np

logger = logging.getLogger('wps.helpers')

DEFAULT_QUEUE = {
    'queue': 'priority.high',
    'exchange': 'priority',
    'routing_key': 'high',
}

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

def json_dumps_default(x):
    if isinstance(x, slice):
        return {
            'type': 'slice',
            'start': x.start,
            'stop': x.stop,
            'step': x.step,
        }
    else:
        raise TypeError()

def json_loads_object_hook(x):
    if 'type' in x:
        if x['type'] == 'slice':
            return slice(x['start'], x['stop'], x['step'])

    return x
