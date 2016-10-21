import os
import sys

import esgf

class ESGFOperation(object):
    """ ESGFOperation

    Defines an ESGFOperation
    """
    def __init__(self):
        file_path = sys.modules[self.__module__].__file__

        self._identifier = '.'.join(os.path.splitext(file_path)[0].split('/')[-2:])

        self._output = None
        
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

    def set_output(self, uri, var_name, mime_type=None):
        """ Sets process output. """
        self._output = esgf.Variable(uri, var_name, mime_type=mime_type)

    def __call__(self, operations, auth, status):
        """ Main execution call. """
        raise NotImplementedError
