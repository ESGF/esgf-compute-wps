import cwt

SERVICE_IDENTIFICATION = cwt.ows.service_identification('LLNL Compute WPS', 'Providing compute resources for ESGF') 
CONTACT = cwt.ows.service_contact()

SERVICE_PROVIDER = cwt.ows.service_provider('LLNL', CONTACT)

GET_CAPABILITIES = lambda x: cwt.ows.operation('GetCapabilities',
                                               x.WPS_ENDPOINT, x.WPS_ENDPOINT)

DESCRIBE_PROCESS = lambda x: cwt.ows.operation('DescribeProcess',
                                               x.WPS_ENDPOINT, x.WPS_ENDPOINT)

EXECUTE = lambda x: cwt.ows.operation('Execute', x.WPS_ENDPOINT, x.WPS_ENDPOINT)

OPERATIONS_METADATA = lambda x: cwt.ows.operations_metadata([GET_CAPABILITIES(x),
                                                             DESCRIBE_PROCESS(x),
                                                             EXECUTE(x)])

def process_descriptions_from_processes(settings, processes):
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

def generate_capabilities(settings, process_offerings):
    args = [
        SERVICE_IDENTIFICATION,
        SERVICE_PROVIDER,
        OPERATIONS_METADATA(settings),
        process_offerings,
        settings.WPS_LANG,
        settings.WPS_VERSION
    ]

    capabilities = cwt.wps.capabilities(*args)
    
    cwt.bds.reset()

    return capabilities.toxml(bds=cwt.bds)

def exception_report(settings, message, code):
    ex = cwt.ows.exception(message, code)

    report = cwt.ows.exception_report(settings.WPS_VERSION, [ex])

    cwt.bds.reset()

    return report.toxml(bds=cwt.bds)

