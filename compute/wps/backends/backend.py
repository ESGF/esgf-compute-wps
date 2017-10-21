from django import db

from wps import models
from wps import wps_xml

__all__ = ['Backend']

class BackendMeta(type):
    def __init__(cls, name, bases, dict):
        if not hasattr(cls, 'registry'):
            cls.registry = {}
        else:
            cls.registry[name] = cls()

        cls.name = name

        return type.__init__(cls, name, bases, dict)

    def get_backend(cls, name):
        return cls.registry.get(name, None)

class Backend(object):
    __metaclass__ = BackendMeta

    def add_process(self, identifier, name, backend, abstract=None):
        server = models.Server.objects.get(host='default')

        if abstract is None:
            abstract = ''

        desc = wps_xml.describe_process_response(identifier, name, abstract)

        try:
            process = models.Process.objects.create(identifier=identifier, backend=backend, description=desc.xml())
        except db.IntegrityError:
            pass
        else:
            process.server_set.add(server)

            process.save()

    def initialize(self):
        pass

    def populate_processes(self):
        pass

    def execute(self, identifier, variables, domains, operations, **kwargs):
        pass
