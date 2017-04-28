#! /usr/bin/env python

import datetime
import logging

from cwt.wps_lib import metadata
from cwt.wps_lib import operations
from lxml import etree

from wps import settings

logger = logging.getLogger(__name__)

class CDAS2ConversionError(Exception):
    pass

def create_identification():
    ident = metadata.ServiceIdentification()

    ident.service_type = settings.SERVICE
    ident.service_type_version = [settings.VERSION]
    ident.title = settings.TITLE

    return ident

def create_provider():
    provider = metadata.ServiceProvider()

    provider.provider_name = settings.NAME
    provider.provider_site = settings.SITE

    return provider

def create_languages():
    languages = metadata.Languages()

    languages.default = settings.LANG
    languages.supported = [settings.LANG]

    return languages

def create_operations():
    # TODO make the addresses host specific
    get_capabilities = metadata.Operation()
    get_capabilities.name = 'GetCapabilities'
    get_capabilities.get = settings.ENDPOINT
    get_capabilities.post = settings.ENDPOINT
    
    describe_process = metadata.Operation()
    describe_process.name = 'DescribeProcess'
    describe_process.get = settings.ENDPOINT
    describe_process.post = settings.ENDPOINT

    execute = metadata.Operation()
    execute.name = 'Execute'
    execute.get = settings.ENDPOINT
    execute.post = settings.ENDPOINT

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

def capabilities_response(data, add_procs=None):
    if add_procs is None:
        add_procs = []

    cap = operations.GetCapabilitiesResponse()

    cap.service = settings.SERVICE
    cap.version = settings.VERSION
    cap.update_sequence = 0
    cap.lang = settings.LANG
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

    for p in add_procs:
        proc = metadata.Process()
        proc.identifier = p.identifier
        proc.title = p.identifier.title()
        proc.abstract = ''

        cap.process_offerings.append(proc)
    
    return cap

def describe_process_response(identifier, title, abstract):
    fmt = metadata.Format(mime_type='text/json')

    cdd = {
           'default': fmt,
           'supported': [fmt],
           'maximum_megabytes': 0,
          }

    complex_data = metadata.ComplexDataDescription(**cdd)

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

    dct = {
        'identifier': identifier,
        'title': title,
        'abstract': abstract,
        'process_version': '1.0.0',
        'store_supported': True,
        'status_supported': True,
        'input': inputs,
        'output': [output],
    }

    proc_desc = metadata.ProcessDescription(**dct)

    dct = {
        'process_description': [proc_desc],
        'service': settings.SERVICE,
        'version': '1.0.0',
        'lang': settings.LANG,
    }

    desc = operations.DescribeProcessResponse(**dct)

    return desc

def describe_process_response_from_cdas2(data):
    tree = etree.fromstring(data)

    identifier = xpath_result(tree, '/processDescriptions/process/description/@id')

    title = xpath_result(tree, '/processDescriptions/process/description/@title')

    abstract = xpath_result(tree, '/processDescriptions/process/description')

    desc = describe_process_response(identifier, title, abstract)

    return desc

def execute_response(status_location, status, identifier):
    p = metadata.Process()
    p.identifier = identifier
    p.title = identifier

    ex = operations.ExecuteResponse()

    ex.service = settings.SERVICE
    ex.service_instance = 'http://0.0.0.0:8000'
    ex.version = settings.VERSION
    ex.lang = settings.LANG
    ex.status_location = status_location
    ex.process = p
    ex.status = status
    ex.creation_time = datetime.datetime.now()

    return ex

def generate_status(status, message=None, percent=None, exception=None):
    if 'Accepted' in status:
        obj = metadata.ProcessAccepted()
    elif 'Started' in status:
        obj = metadata.ProcessStarted()
    elif 'Paused' in status:
        obj = metadata.ProcessPaused()
    elif 'Succeeded' in status:
        obj = metadata.ProcessSucceeded()
    elif 'Failed' in status:
        exc_report = metadata.ExceptionReport.from_xml(exception)

        obj = metadata.ProcessFailed(exception_report=exc_report)

    return obj

def update_execute_response_exception(old_response, exception):
    ex = operations.ExecuteResponse.from_xml(old_response)

    logger.info('Updating execute response with exception')

    ex.status = metadata.ProcessFailed()
    ex.status.exception_report = exception

    return ex

def update_execute_response_status(old_response, status):
    ex = operations.ExecuteResponse.from_xml(old_response)

    ex.status = status

    return ex

def update_execute_response(old_response, output):
    ex = operations.ExecuteResponse.from_xml(old_response)

    ex.status = metadata.ProcessSucceeded()

    data = metadata.ComplexData(value=output)

    output = metadata.Output(identifier='output', title='Output', data=data)

    ex.add_output(output)

    return ex

def update_execute_cdas2_response(old_response, response):
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

def check_cdas2_error(response):
    try:
        tree = etree.fromstring(response)
    except etree.XMLSyntaxError:
        raise CDAS2ConversionError('Failed to load the response string')

    error = tree.xpath('/response/exceptions/exception')

    if len(error) > 0:
        exc_report = metadata.ExceptionReport(VERSION)

        exc_report.add_exception(metadata.NoApplicableCode, error[0].text)

        return exc_report

    return None

