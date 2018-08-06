import os

import cwt
from django.conf import settings

os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

default_app_config = 'wps.apps.WpsConfig'

class WPSError(Exception):
    def __init__(self, message, *args, **kwargs):
        super(WPSError, self).__init__(message.format(*args, **kwargs))

class AccessError(WPSError):
    def __init__(self, url, error):
        msg = 'Error accessing {}: {}'

        super(AccessError, self).__init__(msg, url, error)

SERVICE_IDENTIFICATION = cwt.ows.service_identification('LLNL Compute WPS', 'Providing compute resources for ESGF')

CONTACT = cwt.ows.service_contact()

SERVICE_PROVIDER = cwt.ows.service_provider('LLNL', CONTACT)

GET_CAPABILITIES = cwt.ows.operation('GetCapabilities', settings.WPS_ENDPOINT, settings.WPS_ENDPOINT)

DESCRIBE_PROCESS = cwt.ows.operation('DescribeProcess', settings.WPS_ENDPOINT, settings.WPS_ENDPOINT)

EXECUTE = cwt.ows.operation('Execute', settings.WPS_ENDPOINT, settings.WPS_ENDPOINT)

OPERATIONS_METADATA = cwt.ows.operations_metadata([GET_CAPABILITIES, DESCRIBE_PROCESS, EXECUTE])

def process_descriptions_from_processes(processes):
    descriptions = []

    for process in processes:
        description = cwt.wps.CreateFromDocument(process.description)
        
        descriptions.append(description.ProcessDescription[0])

    args = [
        settings.WPS_LANG,
        settings.WPS_VERSION,
        descriptions
    ]

    process_descriptions = cwt.wps.process_descriptions(*args)

    cwt.bds.reset()

    return process_descriptions.toxml(bds=cwt.bds)

def generate_capabilities(process_offerings):
    args = [
        SERVICE_IDENTIFICATION,
        SERVICE_PROVIDER,
        OPERATIONS_METADATA,
        process_offerings,
        settings.WPS_LANG,
        settings.WPS_VERSION
    ]

    capabilities = cwt.wps.capabilities(*args)
    
    cwt.bds.reset()

    return capabilities.toxml(bds=cwt.bds)

def exception_report(message, code):
    ex = cwt.ows.exception(message, code)

    report = cwt.ows.exception_report(settings.WPS_VERSION, [ex])

    cwt.bds.reset()

    return report.toxml(bds=cwt.bds)
