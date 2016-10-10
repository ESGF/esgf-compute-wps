from wps import logger
from wps.processes.data_container import DataContainer

from esgf import WPSServerError

from urllib2 import urlparse

import cdms2

import json
import mimetypes

class BaseHandler(object):
    """ DataManager BaseHandler. 
    
    To create a DataManager handler subclass BaseHandler. Register handlers
    for reading and writing from different protocols.
    """
    def __init__(self):
        """ Init. """
        self._read = {}
        self._write = {}

    def register_reader(self, scheme, reader_func):
        """ Register a protocol reader function for a scheme. """
        if scheme not in self._read:
            self._read[scheme] = reader_func

    def register_writer(self, scheme, writer_func):
        """ Register a protocol writer function for a scheme. """
        if scheme not in self._write:
            self._write[scheme] = writer_func

    def read(self, variable, **metadata):
        """ Required override. """
        raise NotImplementedError

    def write(self, uri, data, **metadata):
        """ Required override. """
        raise NotImplementedError

    def reader(self, uri):
        """ Returns the correct reader for the uri scheme. """
        scheme = self._parse_scheme(uri)

        try:
            return self._read[scheme]
        except KeyError:
            raise WPSServerError('No protocol handler for %s', scheme)

    def writer(self, uri):
        """ Returns the correct writer for the uri scheme. """
        scheme = self._parse_scheme(uri)

        try:
            return self._write[scheme]
        except KeyError:
            raise WPSServerError('No protocol handler for %s', scheme)

    def _parse_scheme(self, uri):
        """ Parses uri for the scheme portion. """
        scheme, _, _, _, _, _ = urlparse.urlparse(uri)

        return scheme

class NetCDFHandler(BaseHandler):
    """ NetCDFHandler.

    Write/Read allowed for local filesystem and Read-only for http.
    """
    def __init__(self):
        """ Init. """
        super(NetCDFHandler, self).__init__()

        self.register_reader('file', self._read_generic)
        self.register_reader('http', self._read_generic)

        self.register_writer('file', self._write_file)

    def read(self, variable, **metadata):
        """ Reads a netcdf file. """
        reader = self.reader(variable.uri)

        data = reader(variable, **metadata)

        gridder = None

        if 'gridder' in metadata:
            gridder = metadata['gridder']

        return DataContainer(data[variable.var_name], variable.domains, gridder)
    
    def _read_generic(self, variable, **metadata):
        """ Generic ready function for http or file. """
        return cdms2.open(variable.uri, 'r')

    def write(self, uri, data, **metadata):
        """ Writes a netcdf file. """
        writer = self.writer(uri)

        return writer(uri, data, **metadata)

    def _write_file(self, uri, data, **metadata):
        """ Write function for file. """
        new_file = cdms2.open(uri, 'w')

        new_file.write(data, **metadata)

        new_file.close()

class JSONHandler(BaseHandler):
    """ JSONHandler """
    def __init__(self):
        """ Init. """
        super(JSONHandler, self).__init__()

        self.register_writer('file', self._write_file)

    def read(self, variable, **metadata):
        """ Reads a json file. """
        reader = self.reader(uri)

        return reader.read(variable, **metadata)

    def write(self, uri, data, **metadata):
        """ Writes a json file. """
        writer = self.writer(uri)

        return writer(uri, data, **metadata)

    def _write_file(self, uri, data, **metadata):
        """ File protocol writer. """
        with open(uri[7:], 'w') as new_file:
            json.dump(data, new_file)

class DataManager(object):
    """ DataManager

    Handles determining the correct handler for the file provided by the 
    Variable class.
    """
    HANDLERS = {
        'application/x-netcdf': NetCDFHandler(),
        'application/json': JSONHandler(),
    }
       
    def read(self, variable, **metadata):
        """ Read the file describe by variable. """
        mimetype, _ = mimetypes.guess_type(variable.uri)

        try:
            handler = self.HANDLERS[mimetype]
        except KeyError:
            raise WPSServerError('No handler for %s', mimetype)

        data = handler.read(variable, **metadata)

        return data

    def write(self, uri, data, **metadata):
        """ Write the file described by variable. """
        mimetype, _ = mimetypes.guess_type(uri)

        try:
            handler = self.HANDLERS[mimetype]
        except KeyError:
            raise WPSServerError('No handler for %s', mimetype)

        if uri[0] == '/':
            uri = 'file://' + uri

        handler.write(uri, data, **metadata)
