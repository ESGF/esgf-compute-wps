import json

from django.conf import settings
from jinja2 import Environment, PackageLoader

from compute_wps import models

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

ProcessAccepted = 'ProcessAccepted'
ProcessStarted = 'ProcessStarted'
ProcessPaused = 'ProcessPaused'
ProcessSucceeded = 'ProcessSucceeded'
ProcessFailed = 'ProcessFailed'

StatusChoices = [
    ProcessAccepted,
    ProcessStarted,
    ProcessPaused,
    ProcessSucceeded,
    ProcessFailed,
]

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

def execute(job):
    env = Environment(loader=PackageLoader('compute_wps', 'templates'))

    template = env.get_template('Execute_response.xml')

    status = job.status_set.latest('updated_date')

    try:
        latest_message = status.message_set.latest('created_date')
    except models.Message.DoesNotExist:
        message = ""
        percent = 0
    else:
        message = latest_message.message
        percent = latest_message.percent

    kwargs = {
        'instance_url': settings.WPS_URL,
        'status_url': f'{settings.JOB_URL}/{job.id}.wps',
        'version': job.process.version,
        'identifier': job.process.identifier,
        'abstract': job.process.abstract,
        'status': status.status,
        'created_date': status.updated_date,
        'percent': percent,
        'message': message,
    }

    if status.status == ProcessFailed:
        kwargs["message"] = exception_report(NoApplicableCode, message)

    kwargs.update(json.loads(job.datainputs))

    return template.render(**kwargs)
