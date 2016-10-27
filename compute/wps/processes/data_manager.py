import os
import tempfile
import urlparse
import collections

import cdms2
import esgf
import json
import requests

from wps import logger

class NetCDFHandler(object):
    """ NetCDFHandler

    Handles read/write/metadata operations for NetCDF files.

    Only supports http/file protocols.
    """
    def __init__(self, pem_file):
        """ Init """
        self._pem_file = pem_file

        dodsrc = os.path.join(os.path.expanduser('~'), '.dodsrc')

        cookiejar = os.path.join(os.path.expanduser('~'), '.dods_cookies')

        # Update .dodsrc for netcdf library
        with open(dodsrc, 'w') as new_file:
            new_file.write('HTTP.COOKIEJAR=%s\n' % (cookiejar,))
            new_file.write('HTTP.SSL.VALIDATE=1\n')
            new_file.write('HTTP.SSL.CERTIFICATE=%s\n' % (pem_file,))
            new_file.write('HTTP.SSL.KEY=%s\n' % (pem_file,))
            new_file.write('HTTP.SSL.CAPATH=%s\n' % (pem_file,))

    def _localize(self, uri):
        """ Attempts to localize http file using credentials. """
        local_path = None

        with requests.session() as session:
            session.cert = self._pem_file

            response = session.get(uri)

            # Check for good response status
            if response.status_code != 200:
                response.raise_for_status()

            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                current = 0
                total = None

                if 'Content-Length' in response.headers:
                    total = int(response.headers['Content-Length'])

                # Download file in chunks
                for chunk in response.iter_content(2048):
                    temp_file.write(chunk)

                    current += 2048

                    if total:
                        logger.debug('Localize progress %f',
                                     current*100.0/total) 

                local_path = temp_file.name

                logger.debug('Done localizing file "%s"', local_path)

        return local_path

    def _open_http(self, uri):
        """ Handles opening http files. """
        file_obj = None

        # First attempt to open like dap/http file
        try:
            file_obj = cdms2.open(uri, 'r')
        except cdms2.CDMSError:
            pass
        else:
            return file_obj
            
        # Second attempt treat liek http file and localize
        try:
            local_path = self._localize(uri)

            file_obj = cdms2.open(local_path, 'r')
        except cdms2.CDMSError:
            logger.exception('Failed to open remote netcdf file "%s"', uri)

            raise
        else:
            return file_obj

    def _open_local(self, uri):
        """ Handles opening local files. """
        try:
            file_obj = cdms2.open(uri, 'r')
        except cdms2.CDMSError:
            logger.exception('Failed to open local netcdf file "%s"', uri)
            
            raise
        else:
            return file_obj

    def metadata(self, variable):
        """ Retrieves files metadata, not real data is loaded. """
        scheme, _, _, _, _, _ =  urlparse.urlparse(variable.uri)

        if scheme in ('http', 'https'):
            file_obj = self._open_http(variable.uri)
        elif scheme in ('', 'file'):
            file_obj = self._opne_local(variable.uri)
        else:
            raise esgf.WPSServerError('Unsupported protocol "%s"' % (scheme,))

        return file_obj[variable.var_name]

    def read(self, variable):
        """ Loads actual data. """
        file_var = self.metadata(variable)

        # TODO apply domains when request actual data

        return file_var()

    def write(self, uri, data, var_name):
        """ Writes variable to new NetCDF file. """
        # Check if data is in correct format
        if not isinstance(data, cdms2.tvariable.TransientVariable):
            raise esgf.WPSServerError('Input data not in correct format.')

        logger.debug('Writing "%s" variable to file at "%s"',
                     var_name,
                     uri)

        file_obj = cdms2.open(uri, 'w')

        file_obj.write(data, id=var_name)

        file_obj.close()

class JSONHandler(object):
    """ JSONHandler

    Handlers JSON read/write/metadata operations.
    """
    def __init__(self, pem_file):
        """ Init """
        pass

    def metadata(self, variable):
        """ Retrieves metadata. """
        raise NotImplementedError

    def read(self, variable):
        """ Reads actual data. """
        raise NotImplementedError

    def write(self, uri, data, var_name):
        """ Writes object to json file. """
        with open(uri, 'w') as new_file:
            json.dump(data, new_file)

class DataManager(object):
    """ DataManager

    Supported formats:
        - NetCDF
        - JSON
    """
    handlers = {
        '.nc': NetCDFHandler,
        '.json': JSONHandler,
    }

    def __init__(self, pem):
        """ Init """
        self._pem = pem
        self._pem_temp = None

    def __enter__(self):
        """ Enter method for context management. """
        # Maybe write when need rather than all the time,
        # can still use context manager to guarantee cleanup.
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            self._pem_temp = temp_file.name
            
            temp_file.write(self._pem)

            logger.debug('%s', temp_file.name)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ Exit method for context management. 
        
        Clean up written credentials.
        """
        os.remove(self._pem_temp)

    def _resolve_handler(self, uri):
        """ Determine correct handler for file type. """
        scheme, _, path, _, _, _ = urlparse.urlparse(uri)

        _, ext = os.path.splitext(path)

        try:
            handler = self.handlers[ext](self._pem_temp)
        except KeyError:
            raise esgf.WPSServerError('No data handler for file type "%s"' %
                                      (ext,))
        else:
            return handler

    @property
    def pem_file(self):
        """ Temporary PEM file location. """
        return self._pem_temp

    def metadata(self, variable):
        """ Reads metadata. """
        handler = self._resolve_handler(variable.uri)

        return handler.metadata(variable)

    def read(self, variable):
        """ Reads file contents. """
        handler = self._resolve_handler(variable.uri)

        return handler.read(variable)

    def write(self, uri, data, var_name):
        """ Writes file contents. """
        handler = self._resolve_handler(uri)

        return handler.write(uri, data, var_name)
