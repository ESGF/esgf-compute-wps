from django.http import HttpResponse
from django.http import JsonResponse
from django.http import FileResponse
from django.http import Http404
from django.shortcuts import render
from django.contrib.auth.decorators import login_required

import pywps
from pywps import Pywps

from lxml import etree

from urllib import unquote

from tempfile import NamedTemporaryFile

import os
import re
import json

from wps import logger
from wps.conf import settings
from wps.processes import PROCESSES

pywps.config.loadConfiguration(settings.WPS_CONFIG)

os.environ['PYWPS_PROCESSES'] = settings.PROCESS_DIR

def strip_tag_namespace(tag):
    if re.match('^{.*}', tag):
        return re.sub('^{.*}', '', tag)

    return tag

def execute_process(method, query_string):
    if os.path.exists(settings.WPS_CONFIG):
        logger.info('Pywps with confiuration %s' % (settings.WPS_CONFIG))

        service = Pywps(method, settings.WPS_CONFIG)
    else:
        logger.info('No Pywps configuration')

        service = Pywps(method)

    service_inputs = service.parseRequest(query_string)

    return service.performRequest(processes=PROCESSES)

def index(request):
    return render(request, 'wps/index.html')

def api_processes(request):
    logger.info('Requesting processes')

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

#TODO reenable @login_required
def wps(request):
    if request.method == 'GET':
        # Corrects the query format
        query = request.META['QUERY_STRING']

        # unquote undoes the percent coding, pywps wants the string in ascii but
        # utf-8 -> ascii doesn't preserve double quotes (TODO figure out why?)
        query = unquote(query).decode('utf-8').encode('ascii', 'replace').replace('?', '"')

        service_response = execute_process(pywps.METHOD_GET, query)
    elif request.method == 'POST':
        query = request.POST['document']

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

def status(request, file_name):
    output_path = config.getConfigValue('server', 'outputPath')

    output_file = os.path.join(output_path, file_name)
    
    if not os.path.exists(output_file):
        raise Http404('%s does not exist' % file_name)
    
    return FileResponse(open(output_file, 'r'))
