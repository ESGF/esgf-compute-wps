from wps.conf import settings

from pywps import config

from uuid import uuid4 as uuid

import os
import sys

class ESGFOperation(object):
    """ ESGFOperation

    Defines an ESGFOperation
    """
    def __init__(self):
        file_path = sys.modules[self.__module__].__file__

        self._identifier = '.'.join(os.path.splitext(file_path)[0].split('/')[-2:])

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

    def complete_process(self, variable):
        """ Marks the completion of a WPS process. """
        raise NotImplementedError

    def status(self, message, progress):
        """ Updates the status of a WPS process. """
        raise NotImplementedError

    def __call__(self, operations, auth):
        """ Main execution call. """
        raise NotImplementedError
