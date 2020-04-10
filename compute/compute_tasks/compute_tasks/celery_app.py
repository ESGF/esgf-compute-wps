import datetime
import json

import cwt
from celery import Celery
from celery import signals
from celery.utils.log import get_task_logger
from kombu import serialization

DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

logger = get_task_logger('compute_task.celery')


@signals.import_modules.connect
def import_modules_handler(*args, **kwargs):
    from compute_tasks import base

    base.discover_processes()


def default(obj):
    from compute_tasks.context import operation

    if isinstance(obj, slice):
        data = {
            '__type': 'slice',
            'start': obj.start,
            'stop': obj.stop,
            'step': obj.step,
        }
    elif isinstance(obj, cwt.Variable):
        data = {
            'data': obj.to_dict(),
            '__type': 'variable',
        }
    elif isinstance(obj, cwt.Domain):
        data = {
            'data': obj.to_dict(),
            '__type': 'domain',
        }
    elif isinstance(obj, cwt.Process):
        data = {
            'data': obj.to_dict(),
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
    elif isinstance(obj, operation.OperationContext):
        data = {
            'data': obj.to_dict(),
            '__type': 'operation_context',
        }
    else:
        raise TypeError(type(obj))

    logger.debug('Serialized to %r', data)

    return data


def object_hook(obj):
    from compute_tasks.context import operation

    obj = byteify(obj)

    if '__type' not in obj:
        return obj

    logger.debug('Deserializing %r', obj)

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
    elif obj['__type'] == 'operation_context':
        data = operation.OperationContext.from_dict(obj['data'])

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

app = Celery('compute_tasks')

app.conf.update({
    'broker_url': 'filesystem://',
    'broker_transport_options': {
        'data_folder_in': '/app/broker/out',
        'data_folder_out': '/app/broker/out',
        'data_folder_processed': '/app/broker/processed',
    },
    'event_serializer': 'cwt_json',
    'task_serializer': 'cwt_json',
    'result_serializer': 'cwt_json',
})

app.autodiscover_tasks(['compute_tasks'], related_name='cdat', force=True)
app.autodiscover_tasks(['compute_tasks'], related_name='job', force=True)
