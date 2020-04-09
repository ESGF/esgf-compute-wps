#! /usr/bin/env python

import contextlib
import os
import hashlib
import json
import tempfile
import urllib
import xml.etree.ElementTree as ET

import cdms2
import cdtime
import cwt
import requests
from django.conf import settings
from django.core.cache import cache
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.cache import cache_page
from django.views.decorators.http import require_http_methods

from . import common
from compute_wps import metrics
from compute_wps.exceptions import AccessError
from compute_wps.exceptions import WPSError
from compute_wps.auth import oauth2

logger = common.logger

@require_http_methods(['GET'])
@cache_page(60 * 15)
def providers(request):
    try:
        response = requests.get('https://raw.githubusercontent.com/ESGF/config/master/esgf-prod/esgf_known_providers.xml')

        response.raise_for_status()
    except Exception:
        raise Exception('Error retrieving ESGF known providers list')

    known = []

    try:
        root = ET.fromstring(response.text)

        for child in root.iter('OP'):
            name = child.find('NAME').text

            if name == None:
                continue

            url = child.find('URL').text

            known.append({'name': name, 'url': url}) 
    except Exception:
        raise Exception('Error parsing ESGF known provider list document')

    return JsonResponse({'data': known})
