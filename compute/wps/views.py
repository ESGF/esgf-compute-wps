import datetime
import json
import logging
import random
import re
import string
import StringIO

import cdms2
import cwt
import django
import requests
from cwt.wps_lib import metadata
from django import http
from django.contrib.auth import authenticate
from django.contrib.auth import login
from django.contrib.auth import logout
from django.contrib.auth import update_session_auth_hash
from django.core import serializers
from django.core.mail import send_mail
from django.db import IntegrityError
from django.db.models import Max
from django.db.models import Sum
from django.shortcuts import render
from django.shortcuts import redirect
from django.shortcuts import get_list_or_404
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods
from django.views.static import serve
from openid.consumer import consumer
from openid.consumer.discover import DiscoveryFailure
from openid.extensions import ax, sreg

from wps import forms
from wps import models
from wps import node_manager
from wps import settings
from wps import wps_xml
from wps import processes as process
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

            docs = data['response']['docs']

            logger.debug('ESGF search returned keys {}'.format(docs[0].keys()))

            files = []
            variables = []
            time_freqs = []

            for doc in docs:
                files.extend(doc['url'])

                variables.extend(doc['variable'])
                
                time_freqs.extend(doc['time_frequency'])

            uniq_files = []
            time_ranges = []

            time_pattern = re.compile('.*_([0-9]*)-([0-9]*)\.nc')

            for f in files:
                if 'opendap' in f.lower():
                    url = f.split('|')[0].replace('.html', '')

                    try:
                        time_ranges.extend(time_pattern.match(url).groups())
                    except:
                        raise Exception('Failed to parse time range from url "{}"'.format(url))

                    uniq_files.append(url)

            uniq_files = list(uniq_files)

            time_ranges = sorted(set(time_ranges))

            variables = list(set(variables))

            time_freqs = list(set(time_freqs))

            time_fmt = TIME_FMT.get(time_freqs[0], None)

            if time_fmt is None:
                raise Exception('Could not parse time formats for time frequency "{}"'.format(time_freqs))

            time_start = datetime.datetime.strptime(time_ranges[0], time_fmt)

            time_stop = datetime.datetime.strptime(time_ranges[-1], time_fmt)

            base = process.CWTBaseTask()

            # TODO check if certificate is still valid
            base.set_user_creds(cwd='/tmp', user_id=request.user.id)

            axes = {}

            logger.info('Checking file "{}"'.format(uniq_files[0]))

            try:
                with cdms2.open(uniq_files[0]) as infile:
                    variableHeader = None
                    fileVariables = infile.getVariables()

                    for v in fileVariables:
                        if v.id in variables:
                            variableHeader = v

                            break

                    if variableHeader is None:
                        raise Exception('Failed to match variables in dataset')

                    for x in variableHeader.getAxisList():
                        if x.id not in ('time', 'lat', 'lon'):
                            continue

                        if 'axis' in x.attributes:
                            axes[x.id] = {
                                'id': x.id,
                                'id_alt': x.attributes['axis'].lower(),
                                'start': x[0],
                                'stop': x[-1],
                                'units': x.attributes['units']
                            }
            except:
                logger.exception('Failed to open netcdf file')

                raise Exception('Failed to open netcdf file, might need to check certificate')

            axes['time']['start'] = CDAT_TIME_FMT.format(time_start)

            axes['time']['stop'] = CDAT_TIME_FMT.format(time_stop)

            axes['time']['units'] = time_freqs[0]

            data = {
                'files': uniq_files,
                'variables': variables,
                'axes': axes.values()
            }
    except KeyError as e:
        logger.exception('Missing required parameter')

        return failed({'message': 'Mising required parameter "{}"'.format(e.message)})
    except Exception as e:
        logger.exception('Error retrieving ESGF search results')

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
        dimensions = json.loads(request.POST.get('dimensions', ''))

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
        dimensions = json.loads(request.POST.get('dimensions', ''))

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

        return failed(e.message)
    else:
        #return response
        return success(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def oauth2_callback(request):
    logger.info('OAuth2 callback')

    try:
        oid = request.session.pop('openid')

        oauth_state = request.session.pop('oauth_state')
    except KeyError as e:
        logger.exception('Session did not contain excepted keys (openid, oauth_state)')

        return redirect(settings.LOGIN_URL)

    manager = node_manager.NodeManager()

    try:
        manager.auth_oauth2_callback(oid, request.META['QUERY_STRING'], oauth_state)
    except Exception:
        logger.exception('Error handling OAuth2 callback')

        return redirect(settings.LOGIN_URL)

    return redirect(settings.PROFILE_URL)

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
            logger.exception('Error sending user account creation notice')
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
        'admin': user.is_superuser,
        'local_init': not (user.password == '')
    }

    return data

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_details(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to retieve user details')
    except Exception as e:
        logging.exception('Error retrieving user details')

        return failed(e.message)
    else:
        data = user_to_json(request.user)

        data['expires'] = request.session.get_expiry_date()

        return success(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_stats(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to retrieve user stats')

        stat = request.GET.get('stat', None)

        data = {}

        if stat == 'process':
            processes = data['processes'] = []

            for process_obj in request.user.userprocess_set.all():
                processes.append(process_obj.to_json())
        else:
            files = data['files'] = []

            for file_obj in request.user.userfile_set.all():
                files.append(file_obj.to_json())
    except Exception as e:
        return failed(e.message)
    else:
        return success(data)

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

            update_session_auth_hash(request, request.user)
    except Exception as e:
        logger.exception('Error updating user details')

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
        logger.exception('Error regenerating API key')

        return failed(e.message)
    else:
        return success({'api_key': api_key})

@require_http_methods(['POST'])
@ensure_csrf_cookie
def user_login_openid(request):
    try:
        form = forms.OpenIDForm(request.POST)

        if not form.is_valid():
            raise Exception(form.errors)

        openid_url = form.cleaned_data['openid_url']

        logger.info('Attempting to login user "{}"'.format(openid_url))

        c = consumer.Consumer(request.session, models.DjangoOpenIDStore()) 

        try:
            auth_request = c.begin(openid_url)
        except DiscoveryFailure as e:
            logger.exception('OpenID discovery error')

            raise Exception('OpenID discovery error')

        fetch_request = ax.FetchRequest()

        attributes = [
            ('http://axschema.org/contact/email', 'email'),
            ('http://axschema.org/namePerson', 'fullname'),
            ('http://axschema.org/namePerson/first', 'firstname'),
            ('http://axschema.org/namePerson/last', 'lastname'),
            ('http://axschema.org/namePerson/friendly', 'nickname'),
            ('http://schema.openid.net/contact/email', 'old_email'),
            ('http://schema.openid.net/namePerson', 'old_fullname'),
            ('http://schema.openid.net/namePerson/friendly', 'old_nickname')
        ]

        for attr, alias in attributes:
            fetch_request.add(ax.AttrInfo(attr, alias=alias, required=True))

        auth_request.addExtension(fetch_request)

        url = auth_request.redirectURL(settings.OPENID_TRUST_ROOT, settings.OPENID_RETURN_TO)
    except Exception as e:
        logger.exception('Error logging user in with OpenID')

        return failed(e.message)
    else:
        return success({'redirect': url})

def __handle_openid_attribute_exchange(response):
    attrs = {
        'email': None
    }

    ax_response = ax.FetchResponse.fromSuccessResponse(response)

    if ax_response:
        attrs['email'] = ax_response.get('http://axschema.org/contact/email')[0]

    return attrs

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_login_openid_callback(request):
    try:
        c = consumer.Consumer(request.session, models.DjangoOpenIDStore())

        response = c.complete(request.GET, settings.OPENID_RETURN_TO)

        if response.status == consumer.CANCEL:
            raise Exception('OpenID authentication cancelled')
        elif response.status == consumer.FAILURE:
            raise Exception('OpenID authentication failed: {}'.format(response))
        
        openid_url = response.getDisplayIdentifier()

        attrs = __handle_openid_attribute_exchange(response)

        try:
            user = models.User.objects.get(auth__openid_url=openid_url)
        except models.User.DoesNotExist:
            username = openid_url.split('/')[-1]

            user = models.User.objects.create_user(username, attrs['email'])

            models.Auth.objects.create(openid_url=openid_url, user=user)

        login(request, user)
    except Exception as e:
        logger.exception('Error handling OpenID callback')

        return failed(e.message)
    else:
        return redirect('{}?expires={}'.format(settings.OPENID_CALLBACK_SUCCESS, request.session.get_expiry_date()))

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
            raise Exception('Authentication failed')
    except Exception as e:
        logger.exception('Error logging user in')

        return failed(e.message)
    else:
        data = user_to_json(user)

        data['expires'] = request.session.get_expiry_date();

        return success(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_logout(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in')

        logger.info('Logging user {} out'.format(request.user.username))

        logout(request)
    except Exception as e:
        logger.exception('Error logging user out')

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

        request.session.update(session)
    except Exception as e:
        logger.exception('Error authenticating OAuth2')

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
        logger.exception('Error authenticating MyProxyClient')

        return failed(e.message)
    else:
        return success({
            'type': request.user.auth.type,
            'api_key': request.user.auth.api_key
        })

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
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to regenerate capabilities')

        if not request.user.is_superuser:
            raise Exception('Must be an admin to regenerate capabilities')

        manager = node_manager.NodeManager()

        manager.generate_capabilities()
    except Exception as e:
        logger.exception('Error generating capabilities')

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
        logger.exception('Error retrieving CDAS2 capabilities')

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

            if len(status[-1]['messages']) > 0:
                request.session['updated'] = status[-1]['messages'][-1]['created_date'].strftime(SESSION_TIME_FMT)
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

            if len(status) > 0 and len(status[-1]['messages']) > 0:
                request.session['updated'] = status[-1]['messages'][-1]['created_date'].strftime(SESSION_TIME_FMT)

    except Exception as e:
        logger.exception('Error retrieving job details')

        return failed(e.message)
    else:
        return success(status)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def jobs(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to view job history')

        index = int(request.GET.get('index', 0))

        limit = request.GET.get('limit', None)

        jobs_qs = models.Job.objects.filter(user_id=request.user.id, pk__gt=index)

        job_count = len(jobs_qs)
        
        jobs_qs = jobs_qs.annotate(accepted=Max('status__created_date'))

        jobs_qs = jobs_qs.order_by('accepted')

        if limit is not None:
            limit = int(limit)

            jobs_qs = jobs_qs[index: limit-1]
        else:
            jobs_qs = jobs_qs[index:]

        jobs = list(reversed([
            {
                'id': x.id,
                'elapsed': x.elapsed,
                'created': x.status_set.all().values('created_date').order_by('created_date').first()['created_date']
            } for x in jobs_qs
        ]))
    except Exception as e:
        logger.exception('Error retrieving jobs')

        return failed(e.message)
    else:
        return success({'count': job_count, 'jobs': jobs})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def job_remove_all(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('Must be logged in to remove jobs')

        request.user.job_set.all().delete()
    except Exception as e:
        logger.exception('Error removing jobs')

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
    except Exception as e:
        logger.exception('Error removing job')

        return failed(e.message)
    else:
        return success({'job': job_id})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def stats_files(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('User must be authenticated to retrieve stats')

        if not request.user.is_superuser:
            raise Exception('User must be an admin to retrieve stats')

        data = {}

        files = data['files'] = []

        files_qs = models.File.objects.annotate(count=Sum('requested'))

        for file_obj in files_qs:
            files.append(file_obj.to_json())
    except Exception as e:
        return failed(e.message)
    else:
        return success(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def stats_processes(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('User must be authenticated to retrieve stats')

        if not request.user.is_superuser:
            raise Exception('User must be an admin to retrieve stats')

        data = {}

        processes = data['processes'] = []

        processes_qs = models.Process.objects.all()

        for process_obj in processes_qs:
            processes.append(process_obj.to_json(True))
    except Exception as e:
        return failed(e.message)
    else:
        return success(data)

@require_http_methods(['GET'])
@ensure_csrf_cookie
def notification(request):
    try:
        notification = models.Notification.objects.filter(enabled=True).latest('created_date')
    except models.Notification.DoesNotExist:
        return success({'notification': None})
    else:
        return success({'notification': notification.message})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def forgot_username(request):
    try:
        if 'email' not in request.GET:
            raise Exception('Please provide the email address of the account')

        logger.info('Recovering username for "{}"'.format(request.GET['email']))

        user = models.User.objects.get(email=request.GET['email'])

        try:
            send_mail(settings.FORGOT_USERNAME_SUBJECT,
                      settings.FORGOT_USERNAME_MESSAGE.format(username=user.username, login_url=settings.LOGIN_URL),
                      settings.ADMIN_EMAIL,
                      [user.email])
        except:
            pass
    except models.User.DoesNotExist:
        logger.exception('User does not exist with email "{}"'.format(request.GET['email']))

        return failed('There is not user associated with the email "{}"'.format(request.GET['email']))
    except Exception as e:
        logger.exception('Error recovering username')

        return failed(e.message)
    else:
        return success({'redirect': settings.LOGIN_URL})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def forgot_password(request):
    try:
        if 'username' not in request.GET:
            raise Exception('Please provide the username you are trying to reset the password for')

        username = request.GET['username']

        logger.info('Starting password reset process for user "{}"'.format(username))

        user = models.User.objects.get(username=username)

        extra = json.loads(user.auth.extra)

        token = extra['reset_token'] = ''.join(random.choice(string.ascii_letters + string.digits) for _ in xrange(64))

        extra['reset_expire'] = datetime.datetime.now() + datetime.timedelta(1)

        user.auth.extra = json.dumps(extra, default=lambda x: x.strftime('%x %X'))

        user.auth.save()

        reset_url = '{}?token={}&username={}'.format(settings.PASSWORD_RESET_URL, token, user.username)

        try:
            send_mail(settings.FORGOT_PASSWORD_SUBJECT,
                      '',
                      settings.ADMIN_EMAIL,
                      [user.email],
                      html_message=settings.FORGOT_PASSWORD_MESSAGE.format(username=user.username,
                                                              reset_url=reset_url)
                      )
        except:
            logger.exception('Mail error')

            raise Exception('Error sending reset password email')
    except models.User.DoesNotExist:
        logger.exception('User does not exist with username "{}"'.format(request.GET['username']))

        return failed('Username "{}" does not exist'.format(request.GET['username']))
    except Exception as e:
        logger.exception('Error resetting password for user "{}"'.format(request.GET['username']))

        return failed(e.message)
    else:
        return success({'redirect': settings.LOGIN_URL})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def reset_password(request):
    try:
        token = str(request.GET['token'])

        username = str(request.GET['username'])

        password = str(request.GET['password'])

        logger.info('Resetting password for "{}"'.format(username))

        user = models.User.objects.get(username=username)

        extra = json.loads(user.auth.extra)

        if 'reset_token' not in extra or 'reset_expire' not in extra:
            raise Exception('Invalid reset state')

        expires = datetime.datetime.strptime(extra['reset_expire'], '%x %X')

        if datetime.datetime.now() > expires:
            raise Exception('Reset token has expired')

        if extra['reset_token'] != token:
            raise Exception('Tokens do not match')

        del extra['reset_token']

        del extra['reset_expire']

        user.auth.extra = json.dumps(extra)

        user.auth.save()

        user.set_password(password)

        user.save()

        logger.info('Successfully reset password for "{}"'.format(username))
    except KeyError as e:
        logger.exception('Missing key {}'.format(e))

        return failed('Missing required parameter {}'.format(e))
    except models.User.DoesNotExist:
        logger.exception('User does not exist with username "{}"'.format(request.GET['username']))

        return failed('Username "{}" does not exist'.format(request.GET['username']))
    except Exception as e:
        logger.exception('Error resetting password for user "{}"'.format(request.GET['username']))

        return failed(e.message)
    else:
        return success({'redirect': settings.LOGIN_URL})

@require_http_methods(['GET'])
@ensure_csrf_cookie
def processes(request):
    try:
        if not request.user.is_authenticated:
            raise Exception('User must be authenticated to retrieve processes')

        data = [x.identifier for x in models.Process.objects.all()]
    except Exception as e:
        logger.excpetion('Error retrieving processes')

        return failed(e.message)
    else:
        return success(data)
    
@ensure_csrf_cookie
def output(request, file_name):
    return serve(request, file_name, document_root=settings.OUTPUT_LOCAL_PATH)

@ensure_csrf_cookie
def home(request):
    return render(request, 'wps/index.html')

