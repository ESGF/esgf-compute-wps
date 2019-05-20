from django.conf import settings
from jinja2 import Environment, PackageLoader

# Common
MissingParameterValue = 'MissingParameterValue'
InvalidParameterValue = 'InvalidParameterValue'
NoApplicableCode = 'NoApplicableCode'

# GetCapabilities
InvalidUpdateSequence = 'InvalidUpdateSequence'

# GetCapabilities and Execute
VersionNegotiationFailed = 'VersionNegotiationFailed'

# Execute
NotEnoughStorage = 'NotEnoughStorage'
ServerBusy = 'ServerBusy'
FileSizeExceeded = 'FileSizeExceeded'
StorageNotSupported = 'StorageNotSupported'

def exception_report(code, text):
    env = Environment(loader=PackageLoader('compute_wps', 'templates'))

    template = env.get_template('ExceptionReport_response.xml')

    return template.render(exception_code=code, exception_text=text)

def get_capabilities(processes):
    env = Environment(loader=PackageLoader('compute_wps', 'templates'))

    template = env.get_template('GetCapabilities_response.xml')

    return template.render(processes=processes, 
                           **settings.__dict__['_wrapped'].__dict__)

def describe_process(processes):
    env = Environment(loader=PackageLoader('compute_wps', 'templates'))

    template = env.get_template('DescribeProcess_response.xml')

    return template.render(processes=processes,
                           **settings.__dict__['_wrapped'].__dict__)

def execute(**kwargs):
    env = Environment(loader=PackageLoader('compute_wps', 'templates'))

    template = env.get_template('Execute_response.xml')

    return template.render(**kwargs)
