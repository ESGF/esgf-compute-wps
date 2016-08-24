from django.http import HttpResponse
from django.shortcuts import render

import pywps
from pywps import Pywps

import os
import settings

os.environ['PYWPS_PROCESSES'] = os.path.join(settings.WPS_SERVER_DIR, 'processes')

def view_main(request):
    return render(request, 'wps/index.html')

def wps(request):
    service = Pywps(pywps.METHOD_GET)

    service_inputs = service.parseRequest(request.META['QUERY_STRING'])

    service_response = service.performRequest(service_inputs)

    return HttpResponse(service_response)
