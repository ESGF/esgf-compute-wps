import logging

from django import http
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from wps import node_manager

logger = logging.getLogger(__name__)

@require_http_methods(['GET', 'POST'])
@ensure_csrf_cookie
def wps(request):
    logger.info(request.META)
    
    manager = node_manager.NodeManager()

    try:
        if request.method == 'GET':
            response = manager.handle_get(request.GET)
        elif request.method == 'POST':
            response = manager.handle_post(request.body)
    except node_manager.WPSError as e:
        return http.HttpResponse(e.message) 

    return http.HttpResponse(response, content_type='text/xml')

def status(request, job_id):
    manager = node_manager.NodeManager()

    status = manager.get_status(job_id)

    return http.HttpResponse(status, content_type='text/xml')
