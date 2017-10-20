import json
import StringIO

import cwt
import django
from cwt.wps_lib import metadata
from django import http
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from wps import wps_xml
from wps import node_manager
from wps import settings
from . import common

logger = common.logger

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
                dims.append(cwt.Dimension(name, d['start'], d['stop'], step=d.get('step', 1), crs=cwt.CRS('timestamps')))
            else:
                dims.append(cwt.Dimension(name, d['start'], d['stop'], step=d.get('step', 1)))

        domain = cwt.Domain(dims)

        proc = cwt.Process(identifier=process)

        kwargs = {}

        if regrid != 'None':
            if regrid == 'Gaussian':
                kwargs['gridder'] = cwt.Gridder(grid='gaussian~{}'.format(latitudes))
            elif regrid == 'Uniform':
                kwargs['gridder'] = cwt.Gridder(grid='uniform~{}x{}'.format(longitudes, latitudes))

        wps = cwt.WPS('')

        datainputs = wps.prepare_data_inputs(proc, inputs, domain, **kwargs)

        manager = node_manager.NodeManager()

        result = manager.execute(request.user, process, datainputs)
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
        manager = node_manager.NodeManager()

        api_key, op, identifier, data_inputs = manager.handle_request(request)

        logger.info('Handling WPS request {} for api key {}'.format(op, api_key))

        if op == 'getcapabilities':
            response = manager.get_capabilities()
        elif op == 'describeprocess':
            response = manager.describe_process(identifier)
        else:
            try:
                user = models.User.objects.filter(auth__api_key=api_key)[0]
            except IndexError:
                raise Exception('Unable to find a user with the api key {}'.format(api_key))

            response = manager.execute(user, identifier, data_inputs)
    except node_manager.NodeManagerWPSError as e:
        logger.exception('NodeManager error')

        response = e.exc_report.xml()
    except django.db.ProgrammingError:
        logger.exception('Django error')

        exc_report = metadata.ExceptionReport(settings.VERSION)

        exc_report.add_exception(metadata.NoApplicableCode, 'Database has not been initialized')

        failure = metadata.ProcessFailed(exception_report=exc_report)

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()
    except Exception as e:
        logger.exception('WPS Error')

        exc_report = metadata.ExceptionReport(settings.VERSION)

        exc_report.add_exception(metadata.NoApplicableCode, e.message)

        failure = metadata.ProcessFailed(exception_report=exc_report)

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()

    return http.HttpResponse(response, content_type='text/xml')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regen_capabilities(request):
    try:
        common.authentication_required(request)

        common.authorization_required(request)

        manager = node_manager.NodeManager()

        manager.generate_capabilities()
    except Exception as e:
        logger.exception('Error generating capabilities')

        return common.failed(e.message)
    else:
        return common.success('Regenerated capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')
