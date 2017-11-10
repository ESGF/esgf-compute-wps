import json
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

logger = common.logger

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

    operations, domains, variables = cwt.WPS.parse_data_inputs(data_inputs)

    for variable in variables:
        models.File.track(user, variable)

    server = models.Server.objects.get(host='default')

    job = models.Job.objects.create(server=server, user=user, process=process, extra=data_inputs)

    job.accepted()

    logger.info('Accepted job {}'.format(job.id))

    process_backend = backends.Backend.get_backend(process.backend)

    if process_backend is None:
        job.failed()

        raise Exception('Process backend "{}" does not exist'.format(process.backend))

    operation_dict = dict((x.name, x.parameterize()) for x in operations)

    domain_dict = dict((x.name, x.parameterize()) for x in domains)

    variable_dict = dict((x.name, x.parameterize()) for x in variables)

    process_backend.execute(identifier, variable_dict, domain_dict, operation_dict, user=user, job=job)

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

            parameters = request.POST['parameters']
        except KeyError as e:
            raise Exception('Missing required parameter "{}"'.format(e.message))

        latitudes = request.POST.get('latitudes', None)

        longitudes = request.POST.get('longitudes', None)

        # Javascript stringify on an array creates list without brackets
        dimensions = json.loads(request.POST.get('dimensions', '[]'))

        files = files.split(',')

        inputs = [cwt.Variable(x, variable) for x in files]

        dims = []

        for d in dimensions:
            name = d['id'].split(' ')[0]

            if name in ('time', 't'):
                dims.append(cwt.Dimension(name, d['start'], d['stop'], step=d.get('step', 1)))
            else:
                dims.append(cwt.Dimension(name, d['start'], d['stop'], step=d.get('step', 1)))

        domain = cwt.Domain(dims)

        proc = cwt.Process(identifier=process)

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

        try:
            process = request.POST['process']

            variable = request.POST['variable']

            files = request.POST['files']

            regrid = request.POST['regrid']

            parameters = request.POST['parameters']
        except KeyError as e:
            raise Exception('Missing required key "{}"'.format(e.message))

        latitudes = request.POST.get('latitudes', None)

        longitudes = request.POST.get('longitudes', None)

        # Javascript stringify on an array creates list without brackets
        dimensions = json.loads(request.POST.get('dimensions', '[]'))

        files = files.split(',')

        buf = StringIO.StringIO()

        buf.write("import cwt\nimport time\n\n")

        buf.write("wps = cwt.WPS('{}', api_key='{}')\n\n".format(settings.ENDPOINT, request.user.auth.api_key))

        buf.write("files = [\n")

        for f in files:
            buf.write("\tcwt.Variable('{}', '{}'),\n".format(f, variable))

        buf.write("]\n\n")

        buf.write("proc = wps.get_process('{}')\n\n".format(process))

        if len(dimensions) > 0:
            buf.write("domain = cwt.Domain([\n")

            for d in dimensions:
                logger.info(d.keys())
                name = d['id'].split(' ')[0]

                if name.lower() in ('time', 't'):
                    buf.write("\tcwt.Dimension('{}', '{start}', '{stop}', step={step}, crs=cwt.CRS('timestamps')),\n".format(name, **d))
                else:
                    buf.write("\tcwt.Dimension('{}', {start}, {stop}, step={step}),\n".format(name, **d))

            buf.write("])\n\n")

        if regrid != 'None':
            if regrid == 'Gaussian':
                if latitudes is None:
                    raise Exception('Must provide the number of latitudes for the Gaussian grid')

                grid = 'gaussian~{}'.format(latitudes)
            elif regrid == 'Uniform':
                if latitudes is None or longitudes is None:
                    raise Exception('Must provide the number of latitudes and longitudes for the Uniform grid')

                grid = 'uniform~{}x{}'.format(longitudes, latitudes)

            buf.write("regrid = cwt.Gridder(grid='{}')\n\n".format(grid))

        buf.write("wps.execute(proc, inputs=files")

        if len(dimensions) > 0:
            buf.write(", domain=domain")

        if regrid != 'None':
            buf.write(", gridder=grid")

        parameters = parameters.split(',')

        if len(parameters) > 0:
            for param in parameters:
                key, value = str(param).split('=')

                buf.write(", {}='{}'".format(key, value))
        
        buf.write(")\n\n")

        buf.write("while proc.processing:\n")

        buf.write("\tprint proc.status\n\n")

        buf.write("\ttime.sleep(1)\n\n")

        buf.write("print proc.status")

        _, kernel = process.split('.')

        data = {
            'filename': '{}.py'.format(kernel),
            'text': buf.getvalue()
        }

        response = http.HttpResponse(buf.getvalue(), content_type='text/x-script.phyton')

        response['Content-Disposition'] = 'attachment; filename="{}.py"'.format(kernel)
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
