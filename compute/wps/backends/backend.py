import logging

import cwt
from django import db
from django.conf import settings

from wps import models
from wps import WPSError

logger = logging.getLogger('wps.backends')

__all__ = ['Backend']

class BackendMeta(type):
    def __init__(cls, name, bases, dict):
        if not hasattr(cls, 'registry'):
            cls.registry = {}
        else:
            cls.registry[name] = cls()

        cls.NAME = name

        return type.__init__(cls, name, bases, dict)

    def get_backend(cls, name):
        return cls.registry.get(name, None)

VARIABLE = cwt.wps.data_input_description('variable', 'variable', 'application/json', 1, 1)

DOMAIN = cwt.wps.data_input_description('domain', 'domain', 'application/json', 1, 1)

OPERATION = cwt.wps.data_input_description('operation', 'operation', 'application/json', 1, 1)

DATA_INPUTS = [VARIABLE, DOMAIN, OPERATION]

OUTPUT = cwt.wps.process_output_description('output', 'output', 'application/json')

class Backend(object):
    __metaclass__ = BackendMeta

    def __init__(self):
        self.processes = []

    def load_user(self, user_id):
        try:
            user = models.User.objects.get(pk=user_id)
        except models.User.DoesNotExist:
            raise WPSError('User "{}" does not exist', user_id)

        return user

    def load_job(self, job_id):
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise WPSError('Job {} does not exist', job_id)
        
        return job

    def generate_output_path(self, user, filename):
        job_id = user.job_set.count()

        return '{}/{}/{}/{}.nc'.format(settings.WPS_PUBLIC_PATH, user.id,
                                       job_id, filename)

    def add_process(self, identifier, name, metadata, data_inputs=None, process_outputs=None, abstract=None):
        """ Adds a process/operation to a backend.
    
        Metadata should be formed as follows:
            {
                'inputs': 2,
            }

        Args:
            identifier: A str identifer for the process.
            name: A str name for the process.
            metadata: A dict of metadata.
            data_inputs: A list of cwt.wps.DataInputDescription.
            process_outputs: A list of cwt.wps.ProcessOutputDescription.
            abstract: A str abstract for the process.

        Returns:
            None
        """
        if abstract is None:
            abstract = ''

        if data_inputs is None:
            data_inputs = DATA_INPUTS

        if process_outputs is None:
            process_outputs = [OUTPUT]

        args = [
            identifier,
            identifier,
            '1.0.0',
            process_outputs,
        ]

        kwargs = {
            'metadata': metadata,
            'data_inputs': data_inputs,
            'abstract': abstract,
        }

        description = cwt.wps.process_description(*args, **kwargs)

        descriptions = cwt.wps.process_descriptions('en-US', '1.0.0', [description])

        cwt.bds.reset()

        process = {
            'identifier': identifier,
            'backend': self.NAME,
            'abstract': abstract,
            'description': descriptions.toxml(bds=cwt.bds)
        }

        self.processes.append(process)

    def initialize(self):
        pass

    def populate_processes(self):
        raise NotImplementedError('Must implement populate_processes')

    def execute(self, identifier, variables, domains, operations, **kwargs):
        raise NotImplementedError('Must implement execute')

    def workflow(self, root_op, variables, domains, operations, **kwargs):
        raise NotImplementedError('Workflow not implemented')
