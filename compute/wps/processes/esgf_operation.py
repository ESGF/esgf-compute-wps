import os
import sys

import esgf

from wps import logger
from wps.conf import settings

def create_from_def(definition):
    try:
        op = ESGFOperation.registry[definition.identifier]()
    except KeyError:
        raise esgf.WPSServerError('Unable to find operation "%s"' %
                                  (definition.identifier,))

    op.data = definition

    return op

class ESGFOperationMeta(type):
    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'registry'):
            cls.registry = {}
        else:
            reg_name = '.'.join(cls.__module__.split('.')[-2:])

            cls.registry[reg_name] = cls

        super(ESGFOperationMeta, cls).__init__(name, bases, dct)

class ESGFOperation(object):
    """ ESGFOperation

    Defines an ESGFOperation
    """
    __metaclass__ = ESGFOperationMeta

    def __init__(self):
        file_path = sys.modules[self.__module__].__file__

        self._identifier = '.'.join(os.path.splitext(file_path)[0].split('/')[-2:])

        self._output = None

        # Stores esgf.operation object
        self.data = None

    @property
    def identifier(self):
        return self._identifier

    @property
    def title(self):
        """ Returns operation title. """
        raise NotImplementedError

    @property
    def abstract(self):
        """ Returns operation abstract.

        User this are to described the requirements of an operation.
        """
        return ''

    @property
    def version(self):
        """ Returns version of the operation. """
        return ''

    @property
    def output(self):
        """ Returns process output as a Variable. """
        return self._output

    @property
    def domain(self):
        return self.data.domain

    def input(self):
        return self.data.inputs

    def parameter(self, name, required=True):
        if not self.data.parameters:
            if required:
                raise esgf.WPSServerError('Expecting parameter "%s"' % (name,))
            else:
                return None

        try:
            return self.data.parameters[name]
        except KeyError:
            if not required:
                return None

            raise esgf.WPSServerError('No parameter "%s" passed to operation'
                                      ' "%s"' % (name, self._identifier))

    def parameter_bool(self, name, required=True):
        param = self.parameter(name, required=required)

        if param:
            return bool(param.values[0])

        return param

    def create_dap_url(self, filename):
        url_args = {
            'hostname': settings.DAP_HOSTNAME,
            'port': settings.DAP_PORT,
            'filename': filename,
        }

        return settings.DAP_PATH_FORMAT.format(**url_args)

    def set_output(self, uri, var_name, mime_type=None):
        """ Sets process output. """
        self._output = esgf.Variable(uri, var_name, mime_type=mime_type)

    def __call__(self, data_manager, status):
        """ Main execution call. """
        raise NotImplementedError
