__all__ = ['Backend']

class BackendMeta(type):
    def __init__(cls, name, bases, dict):
        if not hasattr(cls, 'registry'):
            cls.registry = {}
        else:
            cls.registry[name] = cls()

        return type.__init__(cls, name, bases, dict)

    def get_backend(cls, name):
        return cls.registry.get(name, None)

class Backend(object):
    __metaclass__ = BackendMeta

    def initialize(self):
        pass

    def populate_processes(self):
        pass

    def execute(self, identifier, data_inputs, **kwargs):
        pass
