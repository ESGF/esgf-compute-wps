import os
import json
import urllib.request
import urllib.parse
import urllib.error
import re
import logging

import zmq
from cwt import utilities
from django import http
from django.conf import settings
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from owslib import wps

from compute_wps import metrics
from compute_wps import models
from compute_wps.auth import keycloak
from compute_wps.auth import token
from compute_wps.auth import traefik
from compute_wps.exceptions import WPSError
from compute_wps.util import wps_response

logger = logging.getLogger('compute_wps.views.service')

PROVISIONER_FRONTEND = os.environ['PROVISIONER_FRONTEND']

DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

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


def default(obj):
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
    else:
        raise TypeError(type(obj))

    return data


def object_hook(obj):
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

def get_parameter(params, name, required=True):
    """ Gets a parameter from a django QueryDict """

    # Case insesitive
    temp = dict((x.lower(), y) for x, y in list(params.items()))

    if name.lower() not in temp:
        logger.info('Missing required parameter %s', name)

        raise WPSError(name.lower(), code=wps_response.MissingParameterValue)

    param = temp.get(name.lower())

    if required and param is None:
        raise WPSError('Missing required parameter')

    return param


def handle_get_capabilities():
    try:
        data = wps_response.get_capabilities(models.Process.objects.all())
    except Exception as e:
        raise WPSError('{}', e)

    return data


def handle_describe_process(identifiers):
    try:
        processes = models.Process.objects.filter(identifier__in=identifiers)

        data = wps_response.describe_process(processes)
    except Exception as e:
        raise WPSError('{}', e)

    return data


def send_request_provisioner(identifier, data_inputs, job, user, process, status_id):
    context = zmq.Context(1)

    client = context.socket(zmq.REQ)

    SNDTIMEO = os.environ.get('SEND_TIMEOUT', 15)
    RCVTIMEO = os.environ.get('RECV_TIMEOUT', 15)

    client.setsockopt(zmq.SNDTIMEO, SNDTIMEO * 1000)
    client.setsockopt(zmq.RCVTIMEO, RCVTIMEO * 1000)

    client.connect('tcp://{!s}'.format(PROVISIONER_FRONTEND).encode())

    payload = {
        'identifier': identifier,
        'job': job,
        'user': user,
        'process': process,
        'status': status_id,
        'provenance': {
            'wps_identifier': identifier,
            'wps_server': settings.EXTERNAL_WPS_URL,
            'wps_document': utilities.data_inputs_to_document(identifier, data_inputs),
        },
        'data_inputs': data_inputs,
    }

    payload = encoder(payload).encode()

    try:
        client.send_multipart([b'devel', payload])
    except zmq.Again:
        logger.info('Error sending request to provisioner')

        raise WPSError('Error sending request to provisioner, retry in a few minutes.')

    try:
        msg = client.recv_multipart()
    except zmq.Again:
        logger.info('Error receiving acknowledgment from provisioner')

        raise WPSError('Error receiving acknowledgment from provisioner, retry in a few minutes.')
    else:
        if msg[0] == b'ACK':
            logger.info('Accepted job %r', msg)
        elif msg[0] == b'ERR':
            logger.info('Provisioner error %r', msg)

            raise WPSError('Provisioner error, retry in a few minutes.')
        else:
            logger.info('Provisioner responded with an unknown response %r', msg)

            raise WPSError('Unknown response from provisioner.')


REQUIRED_DATA_INPUTS = set(['variable', 'domain', 'operation'])


def handle_execute(meta, identifier, data_inputs):
    data_inputs_keys = set([x.lower() for x in data_inputs.keys()])

    logger.info('DataInputs keys %r', data_inputs_keys)

    # Check that we have all required inputs
    if len(data_inputs_keys ^ REQUIRED_DATA_INPUTS) > 0:
        raise WPSError('{}', ', '.join(REQUIRED_DATA_INPUTS-data_inputs_keys), code=wps_response.MissingParameterValue)

    try:
        if settings.AUTH_TRAEFIK:
            logger.info("Using traefik authentication")

            user = traefik.authenticate(meta)
        elif settings.AUTH_KEYCLOAK:
            logger.info("Using keycloak authentication")

            user = keycloak.authenticate(meta)
        else:
            logger.info("Using token authentication")

            user = token.authenticate(meta)
    except Exception as e:
        raise WPSError(str(e))

    if user is None:
        raise WPSError('Could not authenticate user')

    try:
        process = models.Process.objects.get(identifier=identifier)
    except models.Process.DoesNotExist:
        raise WPSError('Process "{identifier}" does not exist', identifier=identifier)

    job = models.Job.objects.create(process=process, user=user, extra=json.dumps(data_inputs))

    status_id = job.accepted()

    metrics.WPS_JOB_STATE.labels(models.ProcessAccepted).inc()

    try:
        send_request_provisioner(identifier, data_inputs, job.id, user.id, process.id, status_id)
    except WPSError as e:
        job.failed(e)

    return job.report


def handle_get(params, meta):
    """ Handle an HTTP GET request. """
    request = get_parameter(params, 'request', True).lower()

    service = get_parameter(params, 'service', True)

    logger.info('GET request %r, service %r', request, service)

    if request == 'getcapabilities':
        with metrics.WPS_REQUESTS.labels('GetCapabilities', 'GET').time():
            response = handle_get_capabilities()
    elif request == 'describeprocess':
        identifier = get_parameter(params, 'identifier', True).split(',')

        with metrics.WPS_REQUESTS.labels('DescribeProcess', 'GET').time():
            response = handle_describe_process(identifier)
    elif request == 'execute':
        identifier = get_parameter(params, 'identifier', True)

        # Cannot use request.GET, django does not parse the parameter
        # correctly in the form of DataInputs=variable=[];domain=[];operation=[]
        query_string = urllib.parse.unquote(meta['QUERY_STRING'])

        match = re.match('.*datainputs=([^&]*)&?.*', query_string, re.I)

        try:
            split = re.split(';', match.group(1))
        except AttributeError:
            raise WPSError('Failed to parse DataInputs param')

        logger.info('Split DataInputs to %r', split)

        try:
            data_inputs = dict(x.split('=') for x in split)
        except ValueError as e:
            logger.info('Failed to parse DataInputs %r', e)

            raise WPSError('Failed to parse DataInputs param')

        for x in data_inputs.keys():
            data_inputs[x] = json.loads(data_inputs[x])

        with metrics.WPS_REQUESTS.labels('Execute', 'GET').time():
            response = handle_execute(meta, identifier, data_inputs)
    else:
        raise WPSError('Operation "{name}" is not supported', name=request)

    return response


def handle_post(data, meta):
    """ Handle an HTTP POST request.

    NOTE: we only support execute requests as POST for the moment
    """
    try:
        doc = wps.etree.fromstring(data)
    except Exception as e:
        logger.exception('Parse error %r', e)

        raise WPSError('Parse error {!r}', e)

    if 'GetCapabilities' in doc.tag:
        raise WPSError('GetCapabilities POST request is not supported')
    elif 'DescribeProcess' in doc.tag:
        raise WPSError('DescribeProcess POST request is not supported')
    elif 'Execute' in doc.tag:
        wpsns = wps.getNamespace(doc)

        try:
            identifier = doc.find(wps.nspath('Identifier')).text
        except AttributeError:
            raise WPSError('Invalid XML missing Identifier element')

        inputs = doc.findall(wps.nspath('DataInputs/Input', ns=wpsns))

        data_inputs = {}

        for item in inputs:
            input_id_elem = item.find(wps.nspath('Identifier'))

            data = item.find(wps.nspath('Data/ComplexData', ns=wpsns))

            try:
                input_id = input_id_elem.text.lower()
            except AttributeError:
                raise WPSError('Invalid XML missing Identifier element')

            try:
                data_inputs[input_id] = json.loads(data.text)
            except AttributeError:
                raise WPSError('Invalid XML missing ComplexData element')

        with metrics.WPS_REQUESTS.labels('Execute', 'POST').time():
            response = handle_execute(meta, identifier, data_inputs)
    else:
        raise WPSError('Unknown root document tag {!r}', doc.tag)

    return response


@metrics.WPS_ERRORS.count_exceptions()
def handle_request(request):
    """ Convert HTTP request to intermediate format. """
    logger.debug(f'META {request.META!r}')

    if request.method == 'GET':
        return handle_get(request.GET, request.META)
    elif request.method == 'POST':
        return handle_post(request.body, request.META)

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps_entrypoint(request):
    response = None

    try:
        response = handle_request(request)
    except WPSError as e:
        logger.exception('WPSError %r %r', request.method, request.path)

        response = wps_response.exception_report(e.code, str(e))
    except Exception as e:
        logger.exception('Some generic exception %r %r', request.method, request.path)

        error = 'Please copy the error and report on Github: {}'.format(str(e))

        response = wps_response.exception_report(wps_response.NoApplicableCode, error)

    return http.HttpResponse(response, content_type='text/xml')


@require_http_methods(['GET'])
def status(request, job_id):
    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        raise WPSError('Status for job "{job_id}" does not exist', job_id=job_id)

    return http.HttpResponse(job.report, content_type='text/xml')


@require_http_methods(['GET'])
def ping(request):
    return http.HttpResponse('pong')
