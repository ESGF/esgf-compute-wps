import json
import re
import StringIO

import cwt
import django
from cwt import wps_lib
from django import http
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common
from wps import backends
from wps import models
from wps import wps_xml
from wps import settings
from wps import tasks

logger = common.logger

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

class WPSException(Exception):
    def __init__(self, message, exception_type=None):
        if exception_type is None:
            exception_type = wps_lib.NoApplicableCode

        self.message = message

        self.exception_type = exception_type

    def report(self):
        exception_report = wps_lib.ExceptionReport(settings.VERSION)

        exception_report.add_exception(self.exception_type, self.message)

        return exception_report

def get_parameter(params, name):
    """ Gets a parameter from a django QueryDict """

    # Case insesitive
    temp = dict((x.lower(), y) for x, y in params.iteritems())

    if name.lower() not in temp:
        logger.info('Missing required parameter %s', name)

        raise WPSException(name.lower(), wps_lib.MissingParameterValue)

    return temp[name.lower()]

def wps_execute(user, identifier, data_inputs):
    """ WPS execute operation """
    try:
        process = models.Process.objects.get(identifier=identifier)
    except models.Process.DoesNotExist:
        raise Exception('Process "{}" does not exist.'.format(identifier))

    process.track(user)

    base = tasks.CWTBaseTask()

    operations, domains, variables = base.load_data_inputs(data_inputs)

    root_node = None
    is_workflow = False

    # flatten out list of inputs from operations
    op_inputs = [i for op in operations.values() for i in op.inputs]

    # find the root operation, this node will not be an input to any other operation
    for op in operations.values():
        if op.name not in op_inputs:
            if root_node is not None:
                raise Exception('Multiple dangling operations')

            root_node = op

    # considered a workflow if any of the root operations inputs are another operation
    is_workflow = any(i in operations.keys() for i in root_node.inputs)

    for variable in variables.values():
        models.File.track(user, variable)

    server = models.Server.objects.get(host='default')

    job = models.Job.objects.create(server=server, user=user, process=process, extra=data_inputs)

    job.accepted()

    logger.info('Accepted job {}'.format(job.id))

    #operations = dict((x, y.parameterize()) for x, y in operations.iteritems())

    #domains = dict((x, y.parameterize()) for x, y in domains.iteritems())

    #variables = dict((x, y.parameterize()) for x, y in variables.iteritems())

    process_backend = backends.Backend.get_backend(process.backend)

    if process_backend is None:
        job.failed()

        raise Exception('Process backend "{}" does not exist'.format(process.backend))

    if is_workflow:
        process_backend = backends.Backend.get_backend('Local')

        process_backend.workflow(root_node, variables, domains, operations, user=user, job=job)
    else:
        process_backend.execute(identifier, variables, domains, operations, user=user, job=job)

    return job.report

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

    return api_key, operation, identifier, data_inputs

def handle_post(data, params):
    """ Handle an HTTP POST request. 

    NOTE: we only support execute requests as POST for the moment
    """
    try:
        request = wps_lib.ExecuteRequest.from_xml(data)
    except etree.XMLSyntaxError:
        logger.exception('Failed to parse xml request')

        raise Exception('POST request only supported for Execure operation')

    # Build to format [variable=[];domain=[];operation=[]]
    data_inputs = '[{0}]'.format(
        ';'.join('{0}={1}'.format(x.identifier, x.data.value) 
                 for x in request.data_inputs))

    # CDAS doesn't like single quotes
    data_inputs = data_inputs.replace('\'', '\"')

    api_key = params.get('api_key')

    return api_key, 'execute', request.identifier, data_inputs

def handle_request(request):
    """ Convert HTTP request to intermediate format. """
    if request.method == 'GET':
        return handle_get(request.GET)
    elif request.method == 'POST':
        return handle_post(request.body, request.GET)

@require_http_methods(['POST'])
@ensure_csrf_cookie
def execute(request):
    try:
        common.authentication_required(request)

        try:
            process = request.POST['process']

            variable = request.POST['variable']

            files = request.POST['files']

            regrid = request.POST['regrid']
        except KeyError as e:
            raise Exception('Missing required parameter "{}"'.format(e.message))

        parameters = request.POST.get('parameters', None)

        latitudes = request.POST.get('latitudes', None)

        longitudes = request.POST.get('longitudes', None)

        # Javascript stringify on an array creates list without brackets
        dimensions = json.loads(request.POST.get('dimensions', '[]'))

        files = files.split(',')

        inputs = [cwt.Variable(x, variable) for x in files]

        dims = []

        for d in dimensions:
            name = d['id'].split(' ')[0]

            dims.append(cwt.Dimension(name, d['start'], d['stop'], step=d.get('step', 1)))

        domain = cwt.Domain(dims)

        proc = cwt.Process(identifier=process)

        if parameters is not None:
            parameters = parameters.split(',')

            for param in parameters:
                key, value = str(param).split('=')

                proc.add_parameters(cwt.NamedParameter(key, value))

        kwargs = {}

        if regrid != 'None':
            if regrid == 'Gaussian':
                kwargs['gridder'] = cwt.Gridder(grid='gaussian~{}'.format(latitudes))
            elif regrid == 'Uniform':
                kwargs['gridder'] = cwt.Gridder(grid='uniform~{}x{}'.format(longitudes, latitudes))

        wps = cwt.WPS('')

        datainputs = wps.prepare_data_inputs(proc, inputs, domain, **kwargs)

        result = wps_execute(request.user, process, datainputs)
    except Exception as e:
        logger.exception('Error executing job')

        return common.failed(e.message)
    else:
        return common.success({'report': result})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def generate(request):
    try:
        common.authentication_required(request)

        data_inputs = request.POST['datainputs']

        data_inputs = re.sub('\|(domain|operation|variable)=', ';\\1=', data_inputs, 3)

        base = tasks.CWTBaseTask()

        o, d, v = base.load_data_inputs(data_inputs, resolve_inputs=True)

        script = WPSScriptGenerator(v, d, o, request.user)

        kernel = o.values()[0].identifier.split('.')[1]

        data = {
            'filename': '{}.py'.format(kernel),
            'text': script.generate()
        }
    except Exception as e:
        logger.exception('Error generating script using CWT End-user API')

        return common.failed(e.message)
    else:
        return common.success(data)

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    try:
        api_key, op, identifier, data_inputs = handle_request(request)

        logger.info('Handling WPS request {} for api key {}'.format(op, api_key))

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
                raise Exception('Unable to find a user with the api key {}'.format(api_key))

            response = wps_execute(user, identifier, data_inputs)
    except WPSException as e:
        failure = wps_lib.ProcessFailed(exception_report=e.report())

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()
    except Exception as e:
        logger.exception('Anonymous exception converting to WPS Exception Report')

        exc_report = wps_lib.ExceptionReport(settings.VERSION)

        exc_report.add_exception(wps_lib.NoApplicableCode, e.message)

        failure = wps_lib.ProcessFailed(exception_report=exc_report)

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()

    return http.HttpResponse(response, content_type='text/xml')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regen_capabilities(request):
    try:
        common.authentication_required(request)

        common.authorization_required(request)

        server = models.Server.objects.get(host='default')

        processes = server.processes.all()

        cap = wps_xml.capabilities_response(add_procs=processes)

        server.capabilities = cap.xml()

        server.save()
    except Exception as e:
        logger.exception('Error generating capabilities')

        return common.failed(e.message)
    else:
        return common.success('Regenerated capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def status(request, job_id):
    try:
        job = models.Job.objects.get(pk=job_id)
    except models.Job.DoesNotExist:
        raise NodeManagerError('Job {0} does not exist'.format(job_id))

    return http.HttpResponse(job.report, content_type='text/xml')
