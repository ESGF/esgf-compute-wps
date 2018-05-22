import json
import re
import StringIO

import celery
import cwt
import django
from cwt import wps_lib
from django import http
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from lxml import etree

from . import common
from wps import backends
from wps import helpers
from wps import models
from wps import tasks
from wps import wps_xml
from wps import settings
from wps import WPSError

logger = common.logger

class WPSExceptionError(WPSError):
    def __init__(self, message, exception_type):
        exception_report = wps_lib.ExceptionReport(settings.VERSION)

        exception_report.add_exception(exception_type, message)

        self.report = exception_report

        super(WPSExceptionError, self).__init__('WPS Exception')

class WPSScriptGenerator(object):
    def __init__(self, variable, domain, operation, user):
        self.operation = operation

        self.domain = domain

        self.variable = variable

        self.user = user

        self.root_op = None

    def write_header(self, buf):
        buf.write('import cwt\nimport time\n\n')

        buf.write('api_key=\'{}\'\n\n'.format(self.user.auth.api_key))

        buf.write('wps = cwt.WPS(\'{}\', api_key=api_key)\n\n'.format(settings.ENDPOINT))

    def write_variables(self, buf):
        for v in self.variable.values():
            buf.write('var-{} = cwt.Variable(\'{}\', \'{}\')\n'.format(v.name, v.uri, v.var_name))

        buf.write('\n')

    def write_domain(self, buf):
        for d in self.domain.values():
            buf.write('dom-{} = cwt.Domain([\n'.format(d.name))

            for dim in d.dimensions:
                buf.write('\tcwt.Dimension(\'{}\', \'{}\', \'{}\', crs=cwt.VALUES, step=\'{}\'),\n'.format(dim.name, dim.start, dim.end, dim.step))

            buf.write('])\n\n')

    def write_processes(self, buf):
        for o in self.operation.values():
            buf.write('op-{} = wps.get_process(\'{}\')\n\n'.format(o.name, o.identifier))

        op = self.operation.values()[0]

        self.root_op = 'op-{}'.format(op.name)

        buf.write('wps.execute(op-{}, inputs=['.format(op.name))

        l = len(op.inputs) - 1

        for i, v in enumerate(op.inputs):
            buf.write('var-{}'.format(v.name))

            if i < l:
                buf.write(', ')

        if op.domain is not None:
            buf.write('], domain=dom-{}'.format(op.domain.name))

        if 'domain' in op.parameters:
            del op.parameters['domain']

        if 'gridder' in op.parameters:
            g = op.parameters['gridder']

            buf.write(', gridder=cwt.Gridder(\'{}\', \'{}\', \'{}\')'.format(g.tool, g.method, g.grid))

            del op.parameters['gridder']

        for p in op.parameters.values():
            buf.write(', {}={}'.format(p.name, p.values))

        buf.write(')\n\n')

    def write_status(self, buf):
        buf.write('while {}.processing:\n'.format(self.root_op))

        buf.write('\tprint {}.status\n\n'.format(self.root_op))

        buf.write('\ttime.sleep(1)\n\n'.format(self.root_op))

    def write_output(self, buf):
        buf.write('print {}.status\n\n'.format(self.root_op))

        buf.write('print {}.output\n\n'.format(self.root_op))

        buf.write('try:\n\timport vcs\n\timport cdms2\nexcept:\n\tpass\nelse:\n')

        buf.write('\tf = cdms2.open({}.output.uri)\n\n'.format(self.root_op))

        buf.write('\tv = vcs.init()\n\n')

        buf.write('\tv.plot(f[{}.output.var_name])'.format(self.root_op))

    def generate(self):
        buf = StringIO.StringIO()

        self.write_header(buf)

        self.write_variables(buf)

        self.write_domain(buf)

        self.write_processes(buf)

        self.write_status(buf)

        self.write_output(buf)

        data = buf.getvalue()

        buf.close()

        return data

def load_data_inputs(data_inputs, resolve_inputs=False):
    o, d, v = cwt.WPS.parse_data_inputs(data_inputs)

    v = dict((x.name, x) for x in v)

    d = dict((x.name, x) for x in d)

    o = dict((x.name, x) for x in o)

    if resolve_inputs:
        for op in o.values():
            op.resolve_inputs(v, o)

            if op.domain is not None:
                if op.domain not in d:
                    raise WPSError('Missing domain "{name}"', name=op.domain)

                op.domain = d[op.domain]

    return o, d, v

def get_parameter(params, name):
    """ Gets a parameter from a django QueryDict """

    # Case insesitive
    temp = dict((x.lower(), y) for x, y in params.iteritems())

    if name.lower() not in temp:
        logger.info('Missing required parameter %s', name)

        raise WPSExceptionError(name.lower(), wps_lib.MissingParameterValue)

    return temp[name.lower()]

def handle_get(params):
    """ Handle an HTTP GET request. """
    request = get_parameter(params, 'request')

    service = get_parameter(params, 'service')

    api_key = params.get('api_key')

    operation = request.lower()

    identifier = None

    data_inputs = None

    if operation == 'describeprocess':
        identifier = get_parameter(params, 'identifier')
    elif operation == 'execute':
        identifier = get_parameter(params, 'identifier')

        data_inputs = get_parameter(params, 'datainputs')

        # angular2 encodes ; breaking django query_string parsing so the 
        # webapp replaces ; with | and the change is reverted before parsing
        # the datainputs
        data_inputs = re.sub('\|(operation|domain|variable)=', ';\\1=', data_inputs)

    logger.info('Handling GET request "%s" for API key %s', operation, api_key)

    return api_key, operation, identifier, data_inputs

def handle_post(data, params):
    """ Handle an HTTP POST request. 

    NOTE: we only support execute requests as POST for the moment
    """
    try:
        request = wps_lib.ExecuteRequest.from_xml(data)
    except Exception as e:
        raise WPSError('Malformed WPS execute request')

    # Build to format [variable=[];domain=[];operation=[]]
    data_inputs = '[{0}]'.format(
        ';'.join('{0}={1}'.format(x.identifier, x.data.value) 
                 for x in request.data_inputs))

    # CDAS doesn't like single quotes
    data_inputs = data_inputs.replace('\'', '\"')

    api_key = params.get('api_key')

    logger.info('Handling POST request for API key %s', api_key)

    return api_key, 'execute', request.identifier, data_inputs

def handle_request(request):
    """ Convert HTTP request to intermediate format. """
    if request.method == 'GET':
        return handle_get(request.GET)
    elif request.method == 'POST':
        return handle_post(request.body, request.GET)

@require_http_methods(['POST'])
@ensure_csrf_cookie
def generate(request):
    try:
        common.authentication_required(request)

        data_inputs = request.POST['datainputs']

        data_inputs = re.sub('\|(domain|operation|variable)=', ';\\1=', data_inputs, 3)

        o, d, v = load_data_inputs(data_inputs, resolve_inputs=True)

        script = WPSScriptGenerator(v, d, o, request.user)

        kernel = o.values()[0].identifier.split('.')[1]

        data = {
            'filename': '{}.py'.format(kernel),
            'text': script.generate()
        }
    except WPSError as e:
        return common.failed(str(e))
    else:
        return common.success(data)

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    try:
        api_key, op, identifier, data_inputs = handle_request(request)

        if op == 'getcapabilities':
            server = models.Server.objects.get(host='default')

            response = server.capabilities
        elif op == 'describeprocess':
            process = models.Process.objects.get(identifier=identifier)

            response = process.description
        else:
            try:
                user = models.User.objects.filter(auth__api_key=api_key)[0]
            except IndexError:
                raise WPSError('Missing API key for WPS execute request')

            try:
                process = models.Process.objects.get(identifier=identifier)
            except models.Process.DoesNotExist:
                raise WPSError('Process "{identifier}" does not exist', identifier=identifier)

            try:
                operations, domains, variables = load_data_inputs(data_inputs)
            except Exception:
                raise WPSError('Failed to parse datainputs')

            server = models.Server.objects.get(host='default')

            job = models.Job.objects.create(server=server, process=process, user=user, extra=data_inputs)

            job.accepted()

            operation_dict = dict((x, y.parameterize()) for x, y in operations.iteritems())

            variable_dict = dict((x, y.parameterize()) for x, y in variables.iteritems())

            domain_dict = dict((x, y.parameterize()) for x, y in domains.iteritems())

            logger.info('Queueing preprocessing job %s user %s', job.id, user.id)

            tasks.preprocess.s(identifier, variable_dict, domain_dict, operation_dict, user_id=user.id, job_id=job.id).delay()

            response = job.report
    except WPSExceptionError as e:
        failure = wps_lib.ProcessFailed(exception_report=e.report)

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()
    except WPSError as e:
        exc_report = wps_lib.ExceptionReport(settings.VERSION)

        exc_report.add_exception(wps_lib.NoApplicableCode, str(e))

        failure = wps_lib.ProcessFailed(exception_report=exc_report)

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()
    except Exception as e:
        exc_report = wps_lib.ExceptionReport(settings.VERSION)

        exc_report.add_exception(wps_lib.NoApplicableCode, 'Please report this as a bug: {}'.format(str(e)))

        failure = wps_lib.ProcessFailed(exception_report=exc_report)

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()
    finally:
        return http.HttpResponse(response, content_type='text/xml')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regen_capabilities(request):
    try:
        common.authentication_required(request)

        common.authorization_required(request)

        server = models.Server.objects.get(host='default')

        processes = server.processes.all()

        cap = wps_xml.capabilities_response(processes)

        server.capabilities = cap.xml()

        server.save()
    except WPSError as e:
        return common.failed(str(e))
    else:
        return common.success('Regenerated capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def status(request, job_id):
    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        raise WPSError('Status for job "{job_id}" does not exist', job_id=job_id)

    return http.HttpResponse(job.report, content_type='text/xml')

def handle_execute(request, user, job, process):
    try:
        variables = request.POST['variables']

        domains = request.POST['domains']

        operation = request.POST['operation']
    except KeyError as e:
        raise WPSError('Missing required parameter "{name}"', name=e)

    operation = cwt.Process.from_dict(json.loads(operation))

    try:
        process = models.Process.objects.get(identifier=operation.identifier)
    except models.Process.DoesNotExist:
        raise WPSError('Unknown process "{name}"', name=operation.identifier)

    process_backend = backends.Backend.get_backend(process.backend)

    if process_backend is None:
        raise WPSError('Unknown backend "{name}"', name=process.backend)

    variables = dict((x, cwt.Variable.from_dict(y)) for x, y in json.loads(variables).iteritems())

    for variable in variables.values():
        models.File.track(user, variable)

    domains = dict((x, cwt.Domain.from_dict(y)) for x, y in json.loads(domains).iteritems())

    identifier = operation.identifier

    operation = { operation.name: operation }

    process_backend.execute(identifier, variables, domains, operation, user=user, job=job, process=process).delay()

def handle_workflow(request, user, job, process):
    try:
        root_node = request.POST['root_node']

        variables = request.POST['variables']

        domains = request.POST['domains']

        operations = request.POST['operations']
    except KeyError as e:
        raise WPSError('Missing required parameter "{name}"', name=e)

    process_backend = backends.Backend.get_backend('Local')

    root_node = cwt.Process.from_dict(json.loads(root_node))

    variables = dict((x, cwt.Variable.from_dict(y)) for x, y in json.loads(variables).iteritems())

    for variable in variables:
        models.File.track(user, variable)

    domains = dict((x, cwt.Domain.from_dict(y)) for x, y in json.loads(domains).iteritems())

    operations = dict((x, cwt.Process.from_dict(y)) for x, y in json.loads(operations).iteritems())

    process_backend.workflow(root_node, variables, domains, operations, user=user, job=job, process=process).delay()

def handle_ingress(request, user, job, process):
    try:
        chunk_map_raw = request.POST['chunk_map']

        domains = request.POST['domains']

        operation = request.POST['operation']
    except KeyError as e:
        raise WPSError('Missing required parameter "{name}"', name=e)

    backend = backends.Backend.get_backend('Local')

    chunk_map = json.loads(chunk_map_raw, object_hook=helpers.json_loads_object_hook)

    for url, meta in chunk_map.keys():
        models.File.track(user, cwt.Variable(url, meta['variable_name']))

    domains = dict((x, cwt.Domain.from_dict(y)) for x, y in json.loads(domains).iteritems())

    operation = cwt.Process.from_dict(json.loads(operation))

    backend.ingress(chunk_map, domains, operation, user=user, job=job, process=process).delay()

@require_http_methods(['POST'])
def execute(request):
    try:
        execute_type = request.POST['type']

        identifier = request.POST['identifier']

        user_id = request.POST['user_id']

        job_id = request.POST['job_id']
    except KeyError as e:
        logger.error('Error executing missing parameter %s', e)

        return http.HttpResponseBadRequest()

    logger.info('Executing "%s" job %s user %s', execute_type, job_id, user_id)

    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        logger.error('Error missing job id %s', job_id)

        return http.HttpResponseBadRequest()

    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist:
        logger.error('User with id %s does not exist', user_id)

        job.failed()

        return http.HttpResponseBadRequest()

    try:
        process = models.Process.objects.get(identifier=identifier)
    except models.Process.DoesNotExist:
        logger.error('Process with identifier "%s" does not exist', identifier)

        job.failed()

        return http.HttpResponseBadRequest()

    process.track(user)

    try:
        if execute_type == 'execute':
            handle_execute(request, user, job, process)
        elif execute_type == 'workflow':
            handle_workflow(request, user, job, process)
        elif execute_type == 'ingress':
            handle_ingress(request, user, job, process)
        else:
            raise WPSError('Unknown execute type {name}', name=execute_type)
    except WPSError as e:
        job.failed(str(e))

        return http.HttpResponseBadRequest()

    return http.HttpResponse()
