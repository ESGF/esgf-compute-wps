from django.contrib.auth import update_session_auth_hash
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common
from wps import forms

logger = common.logger

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_details(request):
    try:
        common.authentication_required(request)
    except Exception as e:
        logger.exception('Error retrieving user details')

        return common.failed(e.message)
    else:
        return common.success(common.user_to_json(request.user))

@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_stats(request):
    try:
        common.authentication_required(request)

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
        return common.failed(e.message)
    else:
        return common.success(data)

@require_http_methods(['POST'])
@ensure_csrf_cookie
def update(request):
    try:
        common.authentication_required(request)

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

        return common.failed(e.message)
    else:
        return common.success(common.user_to_json(request.user))

@require_http_methods(['GET'])
@ensure_csrf_cookie
def regenerate(request):
    try:
        common.authentication_required(request)

        if request.user.auth.api_key == '':
            raise Exception('Initial API key has not been generate yet, authenticate with MyProxyClient or OAuth2')

        user.auth.api_key = ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(64))

        user.auth.save()

        logger.info('Regenerated API key for "{}"'.format(request.user.username))
    except Exception as e:
        logger.exception('Error regenerating API key')

        return common.failed(e.message)
    else:
        return common.success({'api_key': request.user.auth.api_key})
