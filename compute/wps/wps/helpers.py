#! /usr/bin/env python

from __future__ import division
import logging

from wps import WPSError

logger = logging.getLogger('wps.helpers')

DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

# Set INGRESS_QUEUE to prevent breaking old code
INGRESS_QUEUE = DEFAULT_QUEUE = {
    'queue': 'ingress',
    'exchange': 'ingress',
    'routing_key': 'ingress',
}

QUEUE = {
    'edas': {
        'queue': 'edask',
        'exchange': 'edask',
        'routing_key': 'edask',
    },
    'default': {
        'queue': 'default',
        'exchange': 'default',
        'routing_key': 'default',
    },
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


def queue_from_identifier(identifier):
    module, name = identifier.split('.')

    return QUEUE.get(module.lower(), DEFAULT_QUEUE)
