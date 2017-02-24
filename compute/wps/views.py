from django import http

from wps import tasks
from wps.conf import settings

def wps(request):
    # Need to change to async, we're running locally right now.
    if request.method == 'GET':
        response = tasks.handle_get(request,
                settings.CDAS_HOST,
                settings.CDAS_REQUEST_PORT,
                settings.CDAS_RESPONSE_PORT)
    elif request.method == 'POST':
        response = task.handle_post(request,
                settings.CDAS_HOST,
                settings.CDAS_REQUEST_PORT,
                settings.CDAS_RESPONSE_PORT)

    return http.HttpResponse(response)
