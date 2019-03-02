import json
import logging
import os

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

    def generate_output_path(self, user, job, filename):
        if not filename.endswith('.nc'):
            filename = '{}.nc'.format(filename)

        return os.path.join(settings.WPS_PUBLIC_PATH, str(user.id), str(job.id),
                            filename)

    def get_process(self, identifier):
        for x in self.processes:
            if x['identifier'] == identifier:
                return x

        raise WPSError('Missing process with identifier "{}"', identifier)

    def add_process(self, identifier, metadata=None, abstract=None):
        """ Adds a process/operation to a backend.
    
        Args:
            identifier: A str identifer for the process.
            name: A str name for the process.
            metadata: A dict of metadata.
            abstract: A str abstract for the process.

        Returns:
            None
        """
        kwargs = {
            'identifier': identifier,
            'abstract': abstract or '',
            'backend': self.NAME,
            'metadata': metadata or {},
        }

        self.processes.append(kwargs)

    def load_data_inputs(self, variable_raw, domain_raw, operation_raw):
        variable = {}

        for item in json.loads(variable_raw):
            v = cwt.Variable.from_dict(item)

            variable[v.name] = v

        logger.info('Loaded %r variables', len(variable))

        domain = {}

        for item in json.loads(domain_raw):
            d = cwt.Domain.from_dict(item)

            domain[d.name] = d

        logger.info('Loaded %r domains', len(domain))

        operation = {}

        for item in json.loads(operation_raw):
            o = cwt.Process.from_dict(item)

            operation[o.name] = o

        logger.info('Loaded %r operations', len(operation))

        for o in operation.values():
            if o.domain is not None:
                logger.info('Resolving domain %r', o.domain)

                o.domain = domain[o.domain]

            inputs = []

            for inp in o.inputs:
                if inp in variable:
                    inputs.append(variable[inp])
                elif inp in operation:
                    inputs.append(operation[inp])
                else:
                    kwargs = {
                        'inp': inp,
                        'name': o.name,
                        'id': o.identifier,
                    }

                    raise WPSError('Unabled to resolve input "{inp}" for operation "{name}" - "{id}"', **kwargs)

                logger.info('Resolved input %r', inp)

            o.inputs = inputs

        return variable, domain, operation
