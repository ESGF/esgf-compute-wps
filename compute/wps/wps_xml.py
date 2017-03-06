#! /usr/bin/env python

import logging

from lxml import etree

from esgf.wps_lib import metadata
from esgf.wps_lib import operations

logger = logging.getLogger(__name__)

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

SERVICE = 'WPS'
VERSION = '1.0.0'
UPDATE_SEQUENCE = 0
LANG = 'en-US'
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

    return cap.xml()

def create_execute_response(data):
    return data
