#! /usr/bin/env python

import datetime
import logging

from lxml import etree

from cwt.wps_lib import metadata
from cwt.wps_lib import operations

logger = logging.getLogger(__name__)

SERVICE = 'WPS'
VERSION = '1.0.0'
UPDATE_SEQUENCE = 0
LANG = 'en-US'

STATUS_MAP = {
        metadata.ProcessAccepted: 1,
        metadata.ProcessStarted: 2,
        metadata.ProcessPaused: 3,
        metadata.ProcessSucceeded: 4,
        metadata.ProcessFailed: 5,
        }

STATUS_LIST = (
        metadata.ProcessAccepted,
        metadata.ProcessStarted,
        metadata.ProcessPaused,
        metadata.ProcessSucceeded,
        metadata.ProcessFailed
        )

class CDAS2ConversionError(Exception):
    pass

def status_to_int(status):
    if not isinstance(status, STATUS_LIST):
        raise Exception('Expecting status to be type %s', STATUS_LIST)

    for status_cls, i in STATUS_MAP.iteritems():
        if isinstance(status, status_cls):
            return i

    return None

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

def xpath_result(tree, path):
    result = tree.xpath(path)

    if len(result) == 0:
        return None

    if isinstance(result[0], etree._Element):
        return result[0].text

    return result[0]

def create_capabilities_response(data):
    cap = operations.GetCapabilitiesResponse()

    cap.service = SERVICE
    cap.version = VERSION
    cap.update_sequence = UPDATE_SEQUENCE
    cap.lang = LANG
    cap.service_identification = IDENTIFICATION
    cap.service_provider = PROVIDER
    cap.languages = LANGUAGES
    cap.operations_metadata = OPERATIONS

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
    format = metadata.Format(mime_type='text/json')

    complex_data = metadata.ComplexDataDescription(default=format, supported=[format], maximum_megabytes=0)

    inputs = []
   
    for key in ('variable', 'domain', 'operation'):
        dct = {
                'identifier': key,
                'title': key.title(),
                'min_occurs': 1,
                'max_occurs': 1,
                'value': complex_data
                }

        inputs.append(metadata.InputDescription(**dct))

    dct = {
            'identifier': 'output',
            'title': 'Output',
            'value': complex_data,
            }

    output = metadata.OutputDescription(**dct)

    tree = etree.fromstring(data)

    dct = {
            'identifier': xpath_result(tree, '/processDescriptions/process/description/@id'),
            'title': xpath_result(tree, '/processDescriptions/process/description/@title'),
            'abstract': xpath_result(tree, '/processDescriptions/process/description'),
            'process_version': '1.0.0',
            'store_supported': True,
            'status_supported': True,
            'input': inputs,
            'output': [output],
            }

    proc_desc = metadata.ProcessDescription(**dct)
    
    dct = {
            'process_description': [proc_desc],
            'service': SERVICE,
            'version': '1.0.0',
            'lang': LANG,
            }

    desc = operations.DescribeProcessResponse(**dct)

    return desc

def create_execute_response(status_location, status, identifier):
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

def update_execute_response(old_response, response):
    ex = operations.ExecuteResponse.from_xml(old_response)

    logger.info(response)

    ex.status = metadata.ProcessSucceeded()

    tree = etree.fromstring(response)

    output_data = tree.xpath('/response/outputs/data')

    if len(output_data) > 0:
        data = metadata.ComplexData(value=output_data[0].text)

    output = metadata.Output(identifier='output', title='Output', data=data)

    ex.add_output(output)

    return ex

def convert_cdas2_response(response, **kwargs):
    logger.info('Converting CDAS2 response\n%s', response)

    if 'capabilities' in response:
        result = create_capabilities_response(response)
    elif 'processDescription' in response:
        result = create_describe_process_response(response)
    elif 'response' in response:
        result = create_execute_response(**kwargs)
    else:
        raise CDAS2ConversionError('Unknown response format')

    return result
