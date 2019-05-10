from builtins import str
import json

from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common
from wps import forms
from wps import WPSError

logger = common.logger


@require_http_methods(['GET'])
@ensure_csrf_cookie
def user_details(request):
    try:
        common.authentication_required(request)
    except WPSError as e:
        return common.failed(str(e))
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
    except WPSError as e:
        return common.failed(str(e))
    else:
        return common.success(data)


@require_http_methods(['POST'])
@ensure_csrf_cookie
def update(request):
    try:
        common.authentication_required(request)

        form = forms.UpdateForm(request.POST)

        if not form.is_valid():
            errors = dict((key, value) for key, value in list(form.errors.items()))

            raise WPSError('Invalid form, errors "{error}"', error=json.dumps(errors))

        email = form.cleaned_data['email']

        if email != u'':
            request.user.email = email

            request.user.save()
    except WPSError as e:
        return common.failed(str(e))
    else:
        return common.success(common.user_to_json(request.user))


@require_http_methods(['GET'])
@ensure_csrf_cookie
def regenerate(request):
    try:
        common.authentication_required(request)

        if request.user.auth.api_key == '':
            raise WPSError('Cannot regenerate API key, to generate an API key authenticate '
                           'with MyProxyClient or OAuth2')

        request.user.auth.generate_api_key()

        logger.info('Regenerated API key for "{}"'.format(request.user.username))
    except WPSError as e:
        return common.failed(str(e))
    else:
        return common.success({'api_key': request.user.auth.api_key})
