import logging

from django import db

from wps import models
from wps import wps_xml

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

    def add_process(self, identifier, name, abstract=None):
        if abstract is None:
            abstract = ''

        desc = wps_xml.describe_process_response(identifier, name, abstract)

        process = {
            'identifier': identifier,
            'backend': self.NAME,
            'abstract': abstract,
            'description': desc.xml()
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
