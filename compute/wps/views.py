from django.http import HttpResponse
from django.http import JsonResponse
from django.shortcuts import render

import pywps
from pywps import Pywps

from lxml import etree

from urllib import unquote

from tempfile import NamedTemporaryFile

from wps.processes import load_processes

PROCESSES = load_processes()

import os
import re
import json
import logging
import settings

logger = logging.getLogger(__name__)

def strip_tag_namespace(tag):
    if re.match('^{.*}', tag):
        return re.sub('^{.*}', '', tag)

    return tag

def execute_process(method, query_string):
    if os.path.exists(settings.WPS_CONFIG):
        logging.info('Pywps with confiuration %s' % (settings.WPS_CONFIG))

        service = Pywps(method, settings.WPS_CONFIG)
    else:
        logging.info('No Pywps configuration')

        service = Pywps(method)

    service_inputs = service.parseRequest(query_string)

    return service.performRequest(processes=PROCESSES)

def view_main(request):
    return render(request, 'wps/index.html')

def api_processes(request):
    logging.info('Requesting processes')

    service_response = execute_process(pywps.METHOD_GET, 'version=1.0&service=wps&request=getcapabilities')

    root = etree.fromstring(service_response)

    ns = {
        'wps': 'http://www.opengis.net/wps/1.0.0',
        'ows': 'http://www.opengis.net/ows/1.1',
    }

    process_offerings = root.xpath('/wps:Capabilities/wps:ProcessOfferings/*', namespaces=ns)

    processes = []

    for offering in process_offerings:
        process = {}

        processes.append(process)

        for entry in offering:
            tag_cleaned = strip_tag_namespace(entry.tag)

            process[tag_cleaned] = entry.text    

    return JsonResponse({'processes': processes})

def wps(request):
    if request.method == 'GET':
        # Corrects the query format
        query = request.META['QUERY_STRING']

        # unquote undoes the percent coding, pywps wants the string in ascii but
        # utf-8 -> ascii doesn't preserve double quotes (TODO figure out why?)
        query = unquote(query).decode('utf-8').encode('ascii', 'replace').replace('?', '"')

        service_response = execute_process(pywps.METHOD_GET, query)
    elif request.method == 'POST':
        query = request.read()

        temp_file = NamedTemporaryFile(delete=False)

        # Write file and seek beginning, Pywps wants a file-object,
        # rather than the raw string.
        temp_file.write(query)
        temp_file.flush()
        temp_file.seek(0)

        service_response = execute_process(pywps.METHOD_POST, temp_file)

        temp_file.close()
    else:
        return HttpResponse('%s is an unsupported method.' % (request.method,))

    return HttpResponse(service_response, content_type='text/xml')
