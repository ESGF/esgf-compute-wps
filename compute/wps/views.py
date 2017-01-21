from django import http

from wps import tasks
from wps.conf import settings

class MalformedWPSRequestError(Exception):
    pass

def wps(request):
    if request.method == 'GET':
        response = handle_get(request)
    else:
        response = 'Not Implemented' 

    return http.HttpResponse(response)

def handle_get(request):
    try:
        wps_request = request.GET['request']
    except KeyError:
        raise MalformedWPSRequestError()
    else:
        wps_request = wps_request.lower()

    if wps_request == 'getcapabilities':
        task = tasks.get_capabilities.apply_async((settings.CDAS_HOST,
            settings.CDAS_REQUEST_PORT,
            settings.CDAS_RESPONSE_PORT))

        result = task.get()

        return result
    elif wps_request == 'execute':
        try:
            data_inputs = request.GET['datainputs']
        except KeyError:
            raise MalformedWPSRequestError()

        try:
            identifier = request.GET['Identifier']
        except KeyError:
            raise MalformedWPSRequestError()

        task = tasks.execute.apply_async((settings.CDAS_HOST,
            settings.CDAS_REQUEST_PORT,
            settings.CDAS_RESPONSE_PORT,
            identifier,
            data_inputs))

        result = task.get()

        return result

    return http.HttpResponseForbidden() 
