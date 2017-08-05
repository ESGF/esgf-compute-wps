import datetime
import json
import logging
import re
import StringIO

import django
import requests
from django import http
from django.contrib.auth import authenticate
from django.contrib.auth import login as dlogin
from django.contrib.auth import logout
from django.core import serializers
from django.db import IntegrityError
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

CDAT_TIME_FMT = '{0.year:04d}-{0.month:02d}-{0.day:02d} {0.hour:02d}:{0.minute:02d}:{0.second:02d}.0'

@require_http_methods(['GET'])
@ensure_csrf_cookie
def search_esgf(request):
    if not request.user.is_authenticated:
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    dataset_id = request.GET.get('dataset_id', None)

    index_node = request.GET.get('index_node', None)

    shard = request.GET.get('shard', None)

    query = request.GET.get('query', None)

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
        return http.JsonResponse({'status': 'failure', 'errors': 'Failed to search ESGF'})

    try:
        data = json.loads(response.content)
    except:
        return http.JsonResponse({'status': 'failure', 'errors': 'Failed to load ESGF results'})

    files = []
    variables = []
    time_freq = None

    for doc in data['response']['docs']:
        file_url = None

        if time_freq is None:
            time_freq = doc['time_frequency'][0]

        for item in doc['url']:
            url, mime, text = item.split('|')

            if text == 'OPENDAP':
                file_url = url

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

    return http.JsonResponse({'status': 'success', 'data': data})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def generate(request):
    if not request.user.is_authenticated():
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    process = request.POST['process']

    variable = request.POST['variable']

    files = request.POST['files']

    # Javascript stringify on an array creates list without brackets
    dimensions = json.loads('[{}]'.format(request.POST['dimensions']))

    files = files.split(',')

    buf = StringIO.StringIO()

    buf.write("import cwt\nimport time\n\n")

    buf.write("key = 'YOUR KEY'\n\n")

    buf.write("wps = cwt.WPS('', api_key=key)\n\n")

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

    buf.write("wps.execute(proc, inputs=files")

    if len(dimensions) > 0:
        buf.write(", domain=domain")
    
    buf.write(")\n\n")

    buf.write("while proc.processing:\n")

    buf.write("\tprint proc.status\n\n")

    buf.write("\ttime.sleep(1)\n\n")

    buf.write("print proc.status")

    _, kernel = process.split('.')

    response = http.HttpResponse(buf.getvalue(), content_type='text/x-script.phyton')

    response['Content-Disposition'] = 'attachment; filename="{}.py"'.format(kernel)

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
    form = forms.CreateForm(request.POST)

    if not form.is_valid():
        logger.info('Form is not valid')

        return http.JsonResponse({ 'status': 'failure', 'errors': form.errors })

    username = form.cleaned_data['username']

    email = form.cleaned_data['email']

    openid = form.cleaned_data['openid']

    password = form.cleaned_data['password']

    logger.info('Creating new account for {}'.format(username))

    try:
        user = models.User.objects.create_user(username, email, password)
    except IntegrityError:
        return http.JsonResponse({'status': 'failure', 'errors': 'User already exists'})

    user.save()

    user.auth = models.Auth(openid_url=openid)

    user.auth.save()

    return http.JsonResponse({ 'status': 'success' })

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user(request):
    if not request.user.is_authenticated:
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    data = {
            'id': request.user.id,
            'username': request.user.username,
            'email': request.user.email,
           }

    if request.user.auth is not None:
        if request.user.auth.openid == '':
            oid = openid.OpenID.retrieve_and_parse(request.user.auth.openid_url)

            request.user.auth.openid = oid.response

            request.user.auth.save()

        data['openid'] = request.user.auth.openid_url
        data['type'] = request.user.auth.type
        data['api_key'] = request.user.auth.api_key

    return http.JsonResponse(data)

@require_http_methods(['POST'])
@ensure_csrf_cookie
def update(request):
    if not request.user.is_authenticated():
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    form = forms.UpdateForm(request.POST)

    if form.is_valid():
        email = form.cleaned_data['email']

        openid = form.cleaned_data['openid']

        password = form.cleaned_data['password']

        modified = False

        if email != u'':
            request.user.email = email

            modified = True

        if openid != u'':
            request.user.auth.openid = openid

            modified = True

        if password != u'':
            request.user.set_password(password)

            modified = True

        if modified:
            logger.info('User modified');

            request.user.auth.save()

            request.user.save()
    else:
        logger.error('Update form is invalid')

        return http.JsonResponse({ 'status': 'failure', 'errors': form.errors })

    return http.JsonResponse({'status': 'success'})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regenerate_api_key(request, user_id):
    if not request.user.is_authenticated():
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    manager = node_manager.NodeManager()

    api_key = manager.regenerate_api_key(user_id)

    return http.JsonResponse({'api_key': api_key})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login(request):
    form = forms.LoginForm(request.POST)

    if form.is_valid():
        username = form.cleaned_data['username']

        password = form.cleaned_data['password']

        logger.info('Attempting to login user {}'.format(username))

        user = authenticate(request, username=username, password=password)

        if user is not None:
            logger.info('Authenticate user {}, logging in'.format(username))

            dlogin(request, user)

            return http.JsonResponse({ 'status': 'success', 'expires': request.session.get_expiry_date() })
        else:
            logger.warning('Failed to authenticate user')

            return http.JsonResponse({ 'status': 'failure', 'errors': 'Authentication failed.' })
    else:
        logger.info('Login form is invalid')

        return http.JsonResponse({ 'status': 'failure', 'errors': form.errors })

@require_http_methods(['GET'])
@ensure_csrf_cookie
def logout_view(request):
    if request.user.is_authenticated():
        logger.info('Logging user {} out'.format(request.user.username))

        logout(request)

        return http.JsonResponse({'status': 'success'})

    return http.JsonResponse({'status': 'failed', 'errors': 'User not authenticated'})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_oauth2(request):
    if request.user.is_authenticated():
        form = forms.OpenIDForm(request.POST)

        if form.is_valid():
            oid_url = form.cleaned_data.get('openid')

            logger.info('Authenticating OAuth2 for {}'.format(oid_url))

            manager = node_manager.NodeManager()

            redirect_url, session = manager.auth_oauth2(oid_url)

            #request.session.update(session)

            return http.JsonResponse({'status': 'success', 'redirect': redirect_url})
        else:
            logger.warning('OAuth2 login form is invalid')

            errors = form.errors
    else:
        logger.warning('User is not authenticated')

        errors = 'User not authenticated'

    return http.JsonResponse({'status': 'failed', 'errors': errors})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def login_mpc(request):
    if request.user.is_authenticated():
        form = forms.MPCForm(request.POST)

        if form.is_valid():
            username = form.cleaned_data.get('username')

            password = form.cleaned_data.get('password')

            logger.info('Authenticating MyProxyClient for {}'.format(username))

            manager = node_manager.NodeManager()

            try:
                api_key = manager.auth_mpc(request.user.auth.openid_url, username, password)
            except Exception as e:
                return http.JsonResponse({'status': 'failed', 'errors': e.message})

            return http.JsonResponse({'status': 'success'})
        else:
            logger.warning('MyProxyClient login form is invalid')

            errors = form.errors
    else:
        logger.warning('User is not authenticated')

        errors = 'User not authenticated'

    return http.JsonResponse({'status': 'failed', 'errors': errors})

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    manager = node_manager.NodeManager()

    try:
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
    manager = node_manager.NodeManager()

    manager.generate_capabilities()

    return http.HttpResponse('Regenerated capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def cdas2_capabilities(request):
    manager = node_manager.NodeManager()

    manager.cdas2_capabilities()

    return http.HttpResponse('CDAS capabilities')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')

@require_http_methods(['GET'])
@ensure_csrf_cookie
def servers(request):
    servers = models.Server.objects.all()

    data = dict((x.id, {
                        'host': x.host,
                        'added': x.added_date,
                        'status': x.status,
                        'capabilities': x.capabilities
                       }) for x in servers)

    return http.JsonResponse(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def processes(request, server_id):
    try:
        server = models.Server.objects.get(pk=server_id)
    except models.Server.DoesNotExist as e:
        raise http.JsonResponse({'status': 'failed', 'errors': e.message})

    data = dict((x.id, {
                        'identifier': x.identifier,
                        'backend': x.backend,
                        'description': x.description,
                       }) for x in server.processes.all())

    return http.JsonResponse(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def jobs(request, user_id):
    if not request.user.is_authenticated():
        return http.JsonResponse({'status': 'failed', 'errors': 'User not logged in.'})

    try:
        user = models.User.objects.get(pk=user_id)
    except models.User.DoesNotExist as e:
        return http.JsonResponse({'status': 'failed', 'errors': e.message})

    jobs = [{
             'id': x.id,
             'server': x.server.id,
             'elapsed': x.elapsed,
             'status': [{
                         'id': y.id,
                         'created': y.created_date,
                         'status': y.status,
                         'messages': [{
                                       'id': z.id,
                                       'created': z.created_date,
                                       'message': z.message,
                                       'percent': z.percent,
                                      } for z in y.message_set.all()]
                        } for y in x.status_set.all()]
            } for x in user.job_set.all()]

    return http.JsonResponse(dict(jobs=jobs))

@ensure_csrf_cookie
def output(request, file_name):
    return serve(request, file_name, document_root=settings.OUTPUT_LOCAL_PATH)

@ensure_csrf_cookie
def home(request):
    return render(request, 'wps/index.html')
