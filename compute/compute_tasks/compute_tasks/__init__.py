import datetime
import importlib
import json
import types
import os

import cwt
from celery import Celery
from kombu import serialization

DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f'


class WPSError(Exception):
    def __init__(self, fmt, *args, **kwargs):
        self.msg = fmt.format(*args, **kwargs)

    def __str__(self):
        return self.msg


class AccessError(WPSError):
    def __init__(self, url, error):
        super(AccessError, self).__init__('Access error {!r}: {!s}', url, error)


def default(obj):
    from compute_tasks.context import OperationContext
    from compute_tasks.context import WorkflowOperationContext

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
    from compute_tasks.context import OperationContext
    from compute_tasks.context import WorkflowOperationContext

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


serialization.register('cwt_json', encoder, decoder, 'application/json')

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compute.settings')

app = Celery('compute')

app.config_from_object('compute.settings')

app.conf.event_serializer = 'cwt_json'
app.conf.result_serializer = 'cwt_json'
app.conf.task_serializer = 'cwt_json'

app.autodiscover_tasks(['wps.tasks.cdat',
                        'wps.tasks.edas',
                        'wps.tasks.metrics_',
                        'wps.tasks.job'])
