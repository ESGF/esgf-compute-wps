import json
import urllib
import re
import StringIO

import celery
import cwt
import django
from django import http
from django.conf import settings
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from jinja2 import Environment, PackageLoader
from lxml import etree
from owslib import wps

from . import common
from wps import backends
from wps import helpers
from wps import metrics
from wps import models
from wps import tasks
from wps import WPSError
from wps.util import wps_response

logger = common.logger

def get_parameter(params, name, required=True):
    """ Gets a parameter from a django QueryDict """

    # Case insesitive
    temp = dict((x.lower(), y) for x, y in params.iteritems())

    if name.lower() not in temp:
        logger.info('Missing required parameter %s', name)

        raise WPSError(name.lower(), code=wps_response.MissingParameterValue)

    param = temp.get(name.lower())

    if required and param is None:
        raise WPSError('Missing required parameter')

    return param

def handle_get_capabilities():
    try:
        server = models.Server.objects.get(host='default')

        data = wps_response.get_capabilities(server.processes.all())
    except Exception as e:
        raise WPSError('{}', e)

    return data

def handle_describe_process(identifiers):
    try:
        server = models.Server.objects.get(host='default')

        processes = server.processes.filter(identifier__in=identifiers)

        data = wps_response.describe_process(processes)
    except Exception as e:
        raise WPSError('{}', e)

    return data

def handle_execute(meta, identifier, data_inputs):
    try:
        api_key = meta['HTTP_COMPUTE_TOKEN']
    except KeyError:
        raise WPSError('Missing authorization token, should be passed in HTTP header COMPUTE_TOKEN')

    try:
        user = models.User.objects.filter(auth__api_key=api_key)[0]
    except IndexError:
        raise WPSError('Missing API key for WPS execute request')

    server = models.Server.objects.get(host='default')

    try:
        process = server.processes.get(identifier=identifier)
    except models.Process.DoesNotExist:
        raise WPSError('Process "{identifier}" does not exist', identifier=identifier)

    kwargs = {}

    for id in ('variable', 'domain', 'operation'):
        try:
            data = json.loads(data_inputs[id])
        except KeyError as e:
            raise WPSError('{}', id, code=wps_response.MissingParameterValue)
        except ValueError:
            raise WPSError('{}', id, code=wps_response.InvalidParameterValue)

        if id == 'variable':
            data = [cwt.Variable.from_dict(x) for x in data]
        elif id == 'domain':
            data = [cwt.Domain.from_dict(x) for x in data]
        elif id == 'operation':
            data = [cwt.Process.from_dict(x) for x in data]

        kwargs[id] = dict((x.name, x) for x in data)

    job = models.Job.objects.create(server=server, 
                                    process=process, 
                                    user=user, 
                                    extra=json.dumps(data_inputs))

    # at this point we've accepted the job
    job.accepted()

    logger.info('Acceped job %r', job.id)

    kwargs.update({
        'identifier': identifier,
        'user': user,
        'job': job,
        'process': process,
    })

    try:
        backend = backends.Backend.get_backend(process.backend)

        if backend is None:
            raise WPSError('Unknown backend "{name}"', name=process.backend)

        backend.execute(**kwargs)
    except Exception as e:
        logger.exception('Error executing backend %r', process.backend)

        raise WPSError('{}', e)

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
        query_string = urllib.unquote(meta['QUERY_STRING'])

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

        with metrics.WPS_REQUESTS.labels('Execute', 'GET').time():
            response = handle_execute(meta, identifier, data_inputs)
    else:
        raise WPSError('Operation "{name}" is not supported', name=request)

    return response

def handle_post(data, meta):
    """ Handle an HTTP POST request. 

    NOTE: we only support execute requests as POST for the moment
    """
    logger.info('%r', data)
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
                data_inputs[input_id] = data.text
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
@ensure_csrf_cookie
def wps_entrypoint(request):
    response = None

    try:
        response = handle_request(request)
    except WPSError as e:
        logger.exception('WPSError %r %r', request.method, request.path)

        response = wps_response.exception_report(e.code, e.message)
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
