import datetime
import json
import logging
import re
import StringIO

import cwt
import django
import requests
from django import http
from django.contrib.auth import authenticate
from django.contrib.auth import login
from django.contrib.auth import logout
from django.core import serializers
from django.core.mail import send_mail
from django.db import IntegrityError
from django.db.models import Max
from django.shortcuts import render
from django.shortcuts import redirect
from django.shortcuts import get_list_or_404
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from django.views.static import serve
from cwt.wps_lib import metadata

from wps import forms
from wps import models
from wps import node_manager
from wps import settings
from wps import wps_xml
from wps.auth import openid
from wps.auth import oauth2

logger = logging.getLogger('wps.views')

TIME_FREQ = {
             '3hr': '3 Hours',
             '6hr': '6 Hours',
             'day': 'Daily',
             'mon': 'Monthly',
             'monClim': 'Monthly',
             'subhr': 'Sub Hourly',
             'yr': 'Yearly'
            }

TIME_FMT = {
            '3hr': '%Y%m%d%H%M',
            '6hr': '%Y%m%d%H',
            'day': '%Y%m%d',
            'mon': '%Y%m',
            'monClim': '%Y%m',
            'subhr': '%Y%m%d%H%M%S',
            'yr': '%Y'
           }

SESSION_TIME_FMT = '%Y%m%d%H%M%S'
CDAT_TIME_FMT = '{0.year:04d}-{0.month:02d}-{0.day:02d} {0.hour:02d}:{0.minute:02d}:{0.second:02d}.0'

def success(data=None):
    response = {
        'status': 'success',
        'data': data
    }

    return http.JsonResponse(response)

def failed(error=None):
    response = {
        'status': 'failed',
        'error': error
    }

    return http.JsonResponse(response)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_esgf(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to search ESGF')

        dataset_ids = request.GET['dataset_id']

        index_node = request.GET['index_node']

        shard = request.GET.get('shard', None)

        query = request.GET.get('query', None)

        dataset_ids = dataset_ids.split(',')

        files = []
        variables = []
        time_freq = None

        for dataset_id in dataset_ids:
            logger.info('Searching for dataset {}'.format(dataset_id))

            params = {
                      'type': 'File',
                      'dataset_id': dataset_id,
                      'format': 'application/solr+json',
                      'offset': 0,
                      'limit': 8000
                     }

            if query is not None and len(query.strip()) > 0:
                params['query'] = query.strip()

            if shard is not None and len(shard.strip()) > 0:
                params['shards'] = '{}/solr'.format(shard.strip())
            else:
                params['distrib'] = 'false'

            url = 'http://{}/esg-search/search'.format(index_node)

            try:
                response = requests.get(url, params)
            except:
                raise Exception('Failed to retrieve search results')

            try:
                data = json.loads(response.content)
            except:
                raise Exception('Failed to load JSON response')

            for doc in data['response']['docs']:
                file_url = None

                if time_freq is None:
                    time_freq = doc['time_frequency'][0]
                elif time_freq != doc['time_frequency'][0]:
                    raise Exception('Time frequencies between files do not match')                    

                for item in doc['url']:
                    url, mime, text = item.split('|')

                    if text == 'OPENDAP':
                        file_url = url

                        logger.info(file_url)

                        break

                if file_url is not None:
                    files.append(file_url.replace('.html', '')) 

                    variables.append(doc['variable'][0])
        
            def parse_time(x):
                return re.match('.*_([0-9]*)-([0-9]*)\.nc', x).groups()

            time = sorted(set(y for x in files for y in parse_time(x.split('/')[-1])))

            time_fmt = TIME_FMT.get(time_freq, None)

            start = datetime.datetime.strptime(time[0], time_fmt)

            stop = datetime.datetime.strptime(time[-1], time_fmt)

            data = {
                    'files': files,
                    'time': (CDAT_TIME_FMT.format(start), CDAT_TIME_FMT.format(stop)),
                    'time_units': TIME_FREQ.get(time_freq, None),
                    'variables': list(set(variables))
                   }
    except KeyError as e:
        return failed({'message': 'Mising required parameter "{}"'.format(e.message)})
    except Exception as e:
        return failed(e.message)
    else:
        return success(data)

@require_http_methods(['POST'])
@ensure_csrf_cookie
def execute(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to execute a job');

        process = request.POST['process']

        variable = request.POST['variable']

        files = request.POST['files']

        regrid = request.POST['regrid']

        latitudes = request.POST.get('latitudes', None)

        longitudes = request.POST.get('longitudes', None)

        # Javascript stringify on an array creates list without brackets
        dimensions = json.loads('[{}]'.format(request.POST.get('dimensions', '')))

        files = files.split(',')

        inputs = [cwt.Variable(x, variable) for x in files]

        dims = []

        for d in dimensions:
            name = d['name'].split(' ')[0]

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
        logger.exception('Execute failed')

        return failed(e.message)
    else:
        return success({'report': result})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def generate(request):
    try:
        if not request.user.is_authenticated():
            raise Exception('Must be logged in to generate a script');

        process = request.POST['process']

        variable = request.POST['variable']

        files = request.POST['files']

        regrid = request.POST['regrid']

        latitudes = request.POST.get('latitudes', None)

        longitudes = request.POST.get('longitudes', None)

        # Javascript stringify on an array creates list without brackets
        dimensions = json.loads('[{}]'.format(request.POST.get('dimensions', '')))

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
                name = d['name'].split(' ')[0]

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

        response = http.HttpResponse(buf.getvalue(), content_type='text/x-script.phyton')

        response['Content-Disposition'] = 'attachment; filename="{}.py"'.format(kernel)
    except Exception as e:
        return failed(e.message)
    else:
        return response

@require_http_methods(['GET'])
@ensure_csrf_cookie
def oauth2_callback(request):
    logger.info('OAuth2 callback')

    try:
        oid = request.session.pop('openid')

        oid_response = request.session.pop('openid_response')

        oauth_state = request.session.pop('oauth_state')
    except KeyError as e:
        logger.debug('Session did not contain key "%s"', e.message)

        return redirect('login')

    manager = node_manager.NodeManager()

    manager.auth_oauth2_callback(oid, oid_response, request.META['QUERY_STRING'], oauth_state)

    return redirect('home')

@require_http_methods(['POST'])
@ensure_csrf_cookie
def create(request):
    try:
        form = forms.CreateForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        username = form.cleaned_data['username']

        email = form.cleaned_data['email']

        openid = form.cleaned_data['openid']

        password = form.cleaned_data['password']

        try:
            user = models.User.objects.create_user(username, email, password)
        except IntegrityError:
            raise Exception('User already exists')

        models.Auth.objects.create(openid_url=openid, user=user)

        try:
            send_mail(settings.CREATE_SUBJECT,
                      settings.CREATE_MESSAGE,
                      settings.ADMIN_EMAIL.format(login_url=settings.LOGIN_URL, admin_email=settings.ADMIN_EMAIL),
                      [user.email],
                      html_message=True)
        except Exception:
            pass
    except Exception as e:
        logger.exception('Error creating account')

        return failed(e.message)
    else:
        return success('Successfully created account for "{}"'.format(username))

def user_to_json(user):
    data = {
        'username': user.username,
        'email': user.email,
        'openid': user.auth.openid_url,
        'type': user.auth.type,
        'api_key': user.auth.api_key,
        'admin': user.is_superuser
    }

    return data

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_details(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to retieve user details')

        #oid = openid.OpenID.retrieve_and_parse(request.user.auth.openid_url)

        #request.user.auth.openid = oid.response

        #request.user.auth.save()
    except Exception as e:
        return failed(e.message)
    else:
        return success(user_to_json(request.user))

@require_http_methods(['POST'])
@ensure_csrf_cookie
def update(request):
    try:
        if not request.user.is_authenticated():
            raise Exception('Must be logged in to update account')

        form = forms.UpdateForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        email = form.cleaned_data['email']

        openid = form.cleaned_data['openid']

        password = form.cleaned_data['password']

        if email != u'':
            request.user.email = email

            request.user.save()

        if openid != u'':
            request.user.auth.openid_url = openid

            request.user.auth.save()

        if password != u'':
            request.user.set_password(password)

            request.user.save()
    except Exception as e:
        return failed(e.message)
    else:
        return success(user_to_json(request.user))

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regenerate(request):
    try:
        if not request.user.is_authenticated():
            raise Exception('Must be logged in to regenerate api key')

        manager = node_manager.NodeManager()

        api_key = manager.regenerate_api_key(request.user.pk)
    except Exception as e:
        return failed(e.message)
    else:
        return success({'api_key': api_key})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def user_login(request):
    try:
        form = forms.LoginForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        username = form.cleaned_data['username']

        password = form.cleaned_data['password']

        logger.info('Attempting to login user {}'.format(username))

        user = authenticate(request, username=username, password=password)

        if user is not None:
            logger.info('Authenticate user {}, logging in'.format(username))

            login(request, user)
        else:
            raise Exception('Failed to authenticate user')
    except Exception as e:
        return failed(e.message)
    else:
        return success({'expires': request.session.get_expiry_date()})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_logout(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in')

        logger.info('Logging user {} out'.format(request.user.username))

        logout(request)
    except Exception as e:
        return failed(e.message)
    else:
        return success('Logged out')

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_oauth2(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to authenticate using ESGF OAuth2')

        logger.info('Authenticating OAuth2 for {}'.format(request.user.auth.openid_url))

        manager = node_manager.NodeManager()

        redirect_url, session = manager.auth_oauth2(request.user.auth.openid_url)
    except Exception as e:
        return failed(e.message)
    else:
        return success({'redirect': redirect_url})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_mpc(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to authenticate using ESGF MyProxyClient')

        form = forms.MPCForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        username = form.cleaned_data['username']

        password = form.cleaned_data['password']

        logger.info('Authenticating MyProxyClient for {}'.format(username))

        manager = node_manager.NodeManager()

        manager.auth_mpc(request.user.auth.openid_url, username, password)
    except Exception as e:
        return failed(e.message)
    else:
        return success('Successfully logged into ESGF using MyProxyClient')

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
                logger.exception('Unable to find user with api key {}'.format(api_key))

                raise Exception('Unable to find a user with the api key {}'.format(api_key))

            response = manager.execute(user, identifier, data_inputs)
    except node_manager.NodeManagerWPSError as e:
        logger.exception('Specific WPS error')

        response = e.exc_report.xml()
    except django.db.ProgrammingError:
        logger.exception('Handling Django exception')

        exc_report = metadata.ExceptionReport(settings.VERSION)

        exc_report.add_exception(metadata.NoApplicableCode, 'Database has not been initialized')

        failure = metadata.ProcessFailed(exception_report=exc_report)

        exc_response = wps_xml.execute_response('', failure, '')

        response = exc_response.xml()
    except Exception as e:
        logger.exception('Handling WPS general exception')

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
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to regenerate capabilities')

        if not request.user.is_superuser:
            raise Exception('Must be an admin to regenerate capabilities')

        manager = node_manager.NodeManager()

        manager.generate_capabilities()
    except Exception as e:
        return failed(e.message)
    else:
        return success('Regenerated capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def cdas2_capabilities(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to regenerate capabilities')

        if not request.user.is_superuser:
            raise Exception('Must be an admin to regenerate capabilities')

        manager = node_manager.NodeManager()

        manager.cdas2_capabilities()
    except Exception as e:
        return failed(e.message)
    else:
        return success('Regenerated EDAS capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def job(request, job_id):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to view job details')

        update = request.GET.get('update', False)

        if update:
            updated = request.session.get('updated', None)

            if updated is None:
                updated = datetime.datetime.now()
            else:
                updated = datetime.datetime.strptime(updated, SESSION_TIME_FMT)

            status = [
                {
                    'exception': x.exception,
                    'output': x.output,
                    'status': x.status,
                    'messages': [{
                        'created_date': y.created_date,
                        'percent': y.percent,
                        'message': y.message
                    } for y in x.message_set.filter(created_date__gt=updated)]
                } for x in models.Job.objects.get(pk=job_id).status_set.filter(updated_date__gt=updated)
            ]
        else:
            status = [
                {
                    'created_date': x.created_date,
                    'status': x.status,
                    'exception': x.exception,
                    'output': x.output,
                    'messages': [{
                        'created_date': y.created_date,
                        'percent': y.percent,
                        'message': y.message
                    } for y in x.message_set.all().order_by('created_date')]
                } for x in models.Job.objects.get(pk=job_id).status_set.all().order_by('created_date')
            ]

        request.session['updated'] = datetime.datetime.now().strftime(SESSION_TIME_FMT)
    except Exception as e:
        return failed(e.message)
    else:
        return success(status)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def jobs(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to view job history')

        jobs_count = models.Job.objects.filter(user_id=request.user.id).count()

        index = int(request.GET.get('index', 0))

        limit = int(request.GET.get('limit', jobs_count))

        jobs_qs = models.Job.objects.filter(user_id=request.user.id, pk__gt=index)
        
        jobs_qs = jobs_qs.annotate(accepted=Max('status__created_date'))

        jobs_qs = jobs_qs.order_by('accepted')

        if limit is not None:
            jobs_qs = jobs_qs[index: limit-1]

        jobs = list(reversed([
            {
                'id': x.id,
                'elapsed': x.elapsed,
                'accepted': x.status_set.all().values('created_date').order_by('created_date').first()
            } for x in jobs_qs
        ]))
    except Exception as e:
        logger.exception('')
        return failed(e.message)
    else:
        return success({'count': jobs_count, 'jobs': jobs})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def job_remove_all(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to remove jobs')

        request.user.job_set.all().delete()
    except Exception as e:
        return failed(e.message)
    else:
        return success('Removed all jobs')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def job_remove(request, job_id):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to remove a job')

        job = models.Job.objects.get(pk=job_id)

        if job.user != request.user:
            raise Exception('Must be the owner of the job to remove')

        job.delete()
    except models.Job.DoesNotExist:
        return failed('Job does not exists')
    except Exception as e:
        return failed(e.message)
    else:
        return success({'job': job_id})

@ensure_csrf_cookie
def output(request, file_name):
    return serve(request, file_name, document_root=settings.OUTPUT_LOCAL_PATH)

@ensure_csrf_cookie
def home(request):
    return render(request, 'wps/index.html')

