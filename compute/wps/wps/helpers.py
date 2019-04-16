#! /usr/bin/env python

from __future__ import division
import datetime
import importlib
import json
import logging
import types

import cwt

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


def default(obj):
    from wps.context import OperationContext
    from wps.context import WorkflowOperationContext

    if isinstance(obj, slice):
        data = {
            '__type': 'slice',
            'start': obj.start,
            'stop': obj.stop,
            'step': obj.step,
        }
    elif isinstance(obj, cwt.Variable):
        data = {
            'data': obj.parameterize(),
            '__type': 'variable',
        }
    elif isinstance(obj, cwt.Domain):
        data = {
            'data': obj.parameterize(),
            '__type': 'domain',
        }
    elif isinstance(obj, cwt.Process):
        data = {
            'data': obj.parameterize(),
            '__type': 'process',
        }
    elif isinstance(obj, datetime.timedelta):
        data = {
            'data': {
                'days': obj.days,
                'seconds': obj.seconds,
                'microseconds': obj.microseconds,
            },
            '__type': 'timedelta',
        }
    elif isinstance(obj, datetime.datetime):
        data = {
            'data': obj.strftime(DATETIME_FMT),
            '__type': 'datetime',
        }
    elif isinstance(obj, types.FunctionType):
        data = {
            'data': {
                'module': obj.__module__,
                'name': obj.__name__,
            },
            '__type': 'function',
        }
    elif isinstance(obj, OperationContext):
        data = {
            'data': obj.to_dict(),
            '__type': 'operation_context',
        }
    elif isinstance(obj, WorkflowOperationContext):
        data = {
            'data': obj.to_dict(),
            '__type': 'workflow_operation_context',
        }
    else:
        raise TypeError(type(obj))

    return data


def object_hook(obj):
    from wps.context import OperationContext
    from wps.context import WorkflowOperationContext

    obj = byteify(obj)

    if '__type' not in obj:
        return obj

    if obj['__type'] == 'slice':
        data = slice(obj['start'], obj['stop'], obj['step'])
    elif obj['__type'] == 'variable':
        data = cwt.Variable.from_dict(byteify(obj['data']))
    elif obj['__type'] == 'domain':
        data = cwt.Domain.from_dict(byteify(obj['data']))
    elif obj['__type'] == 'process':
        data = cwt.Process.from_dict(byteify(obj['data']))
    elif obj['__type'] == 'timedelta':
        kwargs = {
            'days': obj['data']['days'],
            'seconds': obj['data']['seconds'],
            'microseconds': obj['data']['microseconds'],
        }

        data = datetime.timedelta(**kwargs)
    elif obj['__type'] == 'datetime':
        data = datetime.datetime.strptime(obj['data'], DATETIME_FMT)
    elif obj['__type'] == 'function':
        data = importlib.import_module(obj['data']['module'])

        data = getattr(data, obj['data']['name'])
    elif obj['__type'] == 'operation_context':
        data = OperationContext.from_dict(obj['data'])
    elif obj['__type'] == 'workflow_operation_context':
        data = WorkflowOperationContext.from_dict(obj['data'])

    return data


def byteify(data):
    if isinstance(data, dict):
        return dict((byteify(x), byteify(y)) for x, y in list(data.items()))
    elif isinstance(data, list):
        return list(byteify(x) for x in data)
    elif isinstance(data, bytes):
        return data.decode()
    else:
        return data


def encoder(x):
    return json.dumps(x, default=default)


def decoder(x):
    return json.loads(x, object_hook=object_hook)
