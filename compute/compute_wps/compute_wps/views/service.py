import os
import json
import urllib.request
import urllib.parse
import urllib.error
import re

import zmq
from django import http
from django.views.decorators.http import require_http_methods
from owslib import wps

from . import common
from compute_wps import helpers
from compute_wps import metrics
from compute_wps import models
from compute_wps.exceptions import WPSError
from compute_wps.util import wps_response

logger = common.logger

PROVISIONER_FRONTEND = os.environ['PROVISIONER_FRONTEND']


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


def send_request_provisioner(identifier, data_inputs, process, user):
    context = zmq.Context(1)

    client = context.socket(zmq.REQ)

    SNDTIMEO = os.environ.get('SEND_TIMEOUT', 15)
    RCVTIMEO = os.environ.get('RECV_TIMEOUT', 15)

    client.setsockopt(zmq.SNDTIMEO, SNDTIMEO * 1000)
    client.setsockopt(zmq.RCVTIMEO, RCVTIMEO * 1000)

    client.connect('tcp://{!s}'.format(PROVISIONER_FRONTEND).encode())

    job_id = str(job.id).encode()

    user_id = str(user.id).encode()

    process_id = str(process.id).encode()

    data_inputs = helpers.encoder(data_inputs).encode()

    try:
        client.send_multipart([b'devel', identifier.encode(), data_inputs, job_id, user_id, process_id])
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
        api_key = meta['HTTP_COMPUTE_TOKEN']
    except KeyError:
        raise WPSError('Missing authorization token, should be passed in HTTP header COMPUTE_TOKEN')

    try:
        user = models.User.objects.filter(auth__api_key=api_key)[0]
    except IndexError:
        raise WPSError('Missing API key for WPS execute request')

    try:
        process = models.Process.objects.get(identifier=identifier)
    except models.Process.DoesNotExist:
        raise WPSError('Process "{identifier}" does not exist', identifier=identifier)

    job = models.Job.objects.create(process=process, user=user, extra=json.dumps(data_inputs))

    job.accepted()

    try:
        send_request_provisioner(identifier, data_inputs, process, user)
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
    if request.method == 'GET':
        return handle_get(request.GET, request.META)
    elif request.method == 'POST':
        return handle_post(request.body, request.META)


@require_http_methods(['GET', 'POST'])
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
