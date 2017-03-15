#! /usr/bin/env python

import datetime
import logging

from lxml import etree

from esgf.wps_lib import metadata
from esgf.wps_lib import operations

logger = logging.getLogger(__name__)

SERVICE = 'WPS'
VERSION = '1.0.0'
UPDATE_SEQUENCE = 0
LANG = 'en-US'

def CDAS2ConversionError(Exception):
    pass

def create_identification():
    id = metadata.ServiceIdentification()

    id.service_type = 'WPS'
    id.service_type_version = ['1.0.0']
    id.title = 'LLNL WPS'

    return id

def create_provider():
    provider = metadata.ServiceProvider()

    provider.provider_name = 'Lawerence Livermore National Laboratory'
    provider.provider_site = 'https://llnl.gov'

    return provider

def create_languages():
    languages = metadata.Languages()

    languages.default = 'en-CA'
    languages.supported = ['en-CA']

    return languages

def create_operations():
    # TODO make the addresses host specific
    get_capabilities = metadata.Operation()
    get_capabilities.name = 'GetCapabilities'
    get_capabilities.get = 'http://0.0.0.0:8000/wps'
    get_capabilities.post = 'http://0.0.0.0:8000/wps'
    
    describe_process = metadata.Operation()
    describe_process.name = 'DescribeProcess'
    describe_process.get = 'http://0.0.0.0:8000/wps'
    describe_process.post = 'http://0.0.0.0:8000/wps'

    execute = metadata.Operation()
    execute.name = 'Execute'
    execute.get = 'http://0.0.0.0:8000/wps'
    execute.post = 'http://0.0.0.0:8000/wps'

    return [get_capabilities, describe_process, execute]

IDENTIFICATION = create_identification()
PROVIDER = create_provider()
LANGUAGES = create_languages()
OPERATIONS = create_operations()

def create_capabilities_response(data):
    cap = operations.GetCapabilitiesResponse()

    cap.service = SERVICE
    cap.version = VERSION
    cap.update_sequence = UPDATE_SEQUENCE
    cap.lang = LANG
    cap.service_identification = IDENTIFICATION
    cap.service_provider = PROVIDER
    cap.languages = LANGUAGES
    cap.operation = OPERATIONS

    tree = etree.fromstring(data)

    proc_elems = tree.xpath('/capabilities/processes/process/description')

    cap.process_offerings = []

    for p in proc_elems:
        proc = metadata.Process()
        proc.identifier = p.attrib.get('id')
        proc.title = p.attrib.get('title')
        proc.abstract = p.text

        cap.process_offerings.append(proc)

    return cap

def create_describe_process_response(data):
    pass

def create_execute_response(status_location=None, status=None, identifier=None):
    p = metadata.Process()
    p.identifier = identifier
    p.title = identifier

    ex = operations.ExecuteResponse()

    ex.service = SERVICE
    ex.service_instance = 'http://0.0.0.0:8000'
    ex.version = VERSION
    ex.lang = LANG
    ex.status_location = status_location
    ex.process = p
    ex.status = status
    ex.creation_time = datetime.datetime.now()

    return ex

def convert_cdas2_response(response, **kwargs):
    logger.info('Converting CDAS2 response\n%s', response)

    if 'capabilities' in response:
        result = create_capabilities_response(response)
    elif 'response' in response:
        result = create_execute_response(**kwargs)
    else:
        raise CDAS2ConversionError('Unknown response format')

    return result
