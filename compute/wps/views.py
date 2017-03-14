import logging

from django import http
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from esgf.wps_lib import metadata

from wps import node_manager

logger = logging.getLogger(__name__)

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    manager = node_manager.NodeManager()

    try:
        response = manager.handle_request(request)
    except node_manager.NodeManagerError as e:
        # NodeManagerError should always contain ExceptionReport xml
        response = e.message
    except Exception as e:
        # Handle any generic exceptions, a catch-all
        report = metadata.ExceptionReport('1.0.0')

        report.add_exception(metadata.NoApplicableCode, e.message)

        response = report.xml()

    return http.HttpResponse(response, content_type='text/xml')

def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')
