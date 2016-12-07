import os
import urlparse
import uuid

import cdms2
import esgf
import requests
from pywps import config

from wps import logger

class DataAccessError(Exception):
    pass

class VariableData(object):
    """ Provides access to a netcdf files data. """
    def __init__(self, variable):
        self._variable = variable

        self._file_obj = None
        self._var_obj = None

        self._spatial_domain = {}

    def _domain_to_selector(self):
        if self._variable.domains:
            for dim in self._variable.domains.dimensions:
                if dim.crs == esgf.Dimension.indices:
                    self._spatial_domain[dim.name] = slice(dim.start, dim.end)
                elif dim.crs == esgf.Dimension.values:
                    self._spatial_domain[dim.name] = (dim.start, dim.end)
                else:
                    raise DataAccessError('Unknown CRS %s when building domain' % (dim.crs,))

    @property
    def variable(self):
        """ Variable definition. """
        return self._variable

    @property
    def cdms_file(self):
        """ Raw CdmsFile object. """
        return self._file_obj

    @property
    def cdms_variable(self):
        """ Raw CdmsVariable object. """
        return self._var_obj

    def load_variable(self, file_obj):
        """ Loads the metadata of a CdmsVariable. """
        self._file_obj = file_obj

        self._var_obj = file_obj[self._variable.var_name]

        self._domain_to_selector()

    def __getitem__(self, val):
        """ Slice into the variable data. """
        if isinstance(val, (list, tuple)):
            return self._var_obj(time=slice(val[0], val[1]), **self._spatial_domain)
        elif isinstance(val, int):
            return self._var_obj(time=val, **self._spatial_domain)
        else:
            raise DataAccessError('Could not retrieve data for "%s"' % (val,))

class DataManager(object):
    """ DataManager

    The data manager handles opening netcdf files through a waterfall 
    strategy in the following order:

    DAP -> DAP w/Credentials -> HTTP -> HTTP w/Credentials

    The HTTP methods will localize the file first then pass to Cdms2 to open
    the netcdf file.

    DAP w/Credentials will write a .dodsrc file for the underlying libnetcdf
    library.

    Attribute:
        esgf_credentials: Path to the credentials.pem file
        esgf_ca_path: Path to CA trust of data server
        http_chunk_size: Chunk size for HTTP connections
        http_timeout: Timeout for HTTP connections
    """
    def __init__(self, esgf_credentials, esgf_ca_path, **kwargs):
        self._credentials = esgf_credentials
        self._ca_path = esgf_ca_path

        self._temp_path = config.getConfigValue('server', 'tempPath', '/tmp/wps')
        self._default_dodsrc = os.path.expanduser('~/.dodsrc')

        self._remote_handlers = [
            self._open_dap,
            self._open_dap_credentials,
            self._open_http,
            self._open_http_credentials,
        ]

        self._http_chunk_size = kwargs.get('http_chunk_size', 10 * 1024)
        self._http_timeout = kwargs.get('http_timeout', 5)

    def _open_local(self, variable):
        """ Opens local netcdf file. """
        var = VariableData(variable)

        file_obj = cdms2.open(variable.uri, 'r')

        var.load_variable(file_obj)

        return var

    def _open_dap(self, variable):
        """ Opens DAP netcdf file. """
        # Let cdms2 handle the dap url
        return self._open_local(variable)

    def _open_dap_credentials(self, variable):
        """ Opens DAP netcdf using ESGF credentials. """
        # Write the dodsrc file
        dodsrc_path = self.write_dodsrc(ssl_certificate = self._credentials,
                                        ssl_key = self._credentials,
                                        ssl_ca_path = self._ca_path)

        # Let cdms2 handle the dap url, uses libnetcdf that should support
        # .dodsrc file
        return self._open_local(variable)

    def _open_http(self, variable, credentials=None, ca_path=None):
        """ Localizes and opens netcdf file over HTTP. """
        res = requests.get(variable.uri,
                           timeout=self._http_timeout,
                           cert=credentials,
                           verify=ca_path)

        if res.status_code != 200:
            res.raise_for_status()

        output_path = os.path.join(self._temp_path, '%s.nc' % (str(uuid.uuid4()),))

        size = 0
        total_size = res.headers.get('Content-Length', None)
        
        with open(output_path, 'w') as output_file:
            for chunk in res.iter_content(self._http_chunk_size):
                output_file.write(chunk)

                size += self._http_chunk_size

                if total_size:
                    logger.debug('Localized %s of %s', size, total_size)
                else:
                    logger.debug('Localized %s bytes so far', size)

        var = VariableData(variable)

        file_obj = cdms2.open(output_path, 'r')

        var.load_variable(file_obj)

        return var

    def _open_http_credentials(self, variable):
        """ Localizes and opens netcdf file over HTTP using ESGF credentials. """
        return self._open_http(variable,
                               credentials=self._credentials,
                               ca_path=self._ca_path)

    def _open_remote(self, variable):
        """ Opening remote file. """
        for handler in self._remote_handlers:
            try:
                var = handler(variable)
            except (DataAccessError, cdms2.error.CDMSError) as e:
                logger.debug('Handler "%s" failed with error "%s"',
                             handler.__name__,
                             e)

                if os.path.exists(self._default_dodsrc):
                    os.remove(self._default_dodsrc)
            else:
                return var

        raise DataAccessError('Unable to access remote file "%s"' %
                              (variable.uri,))

    def write_dodsrc(self, **kwargs):
        """ Write a dodsrc file for libnetcdf library. """
        out_file = kwargs.get('out_file', self._default_dodsrc)
        verbose = kwargs.get('verbose', False)
        cookie_jar = kwargs.get('cookie_jar', os.path.expanduser('~/.dods_cookies'))
        ssl_validate = kwargs.get('ssl_validate', False)
        ssl_certificate = kwargs.get('ssl_certificate', os.path.expanduser('~/.esg/credentials'))
        ssl_key = kwargs.get('ssl_key', os.path.expanduser('~/.esg/credentials'))
        ssl_ca_path = kwargs.get('ssl_ca_path', os.path.expanduser('~/.esg/certificates'))

        def bool2bin(val):
            if val:
                return 1

            return 0

        with open(out_file, 'w') as dodsrc:
            dodsrc.write('HTTP.VERBOSE=%s\n' % (bool2bin(verbose),))
            dodsrc.write('HTTP.COOKIEJAR=%s\n' % (cookie_jar,))
            dodsrc.write('HTTP.SSL.VALIDATE=%s\n' % (bool2bin(ssl_validate),))
            dodsrc.write('HTTP.SSL.CERTIFICATE=%s\n' % (ssl_certificate,))
            dodsrc.write('HTTP.SSL.KEY=%s\n' % (ssl_key,))
            dodsrc.write('HTTP.SSL.CAPATH=%s\n' % (ssl_ca_path,))

        return out_file

    def open(self, variable):
        """ Opens a netcdf file base on URI scheme. """
        scheme, _, _, _, _, _ = urlparse.urlparse(variable.uri)

        if scheme in ('', 'file'):
            data_file = self._open_local(variable)
        elif scheme in ('http', 'https'):
            data_file = self._open_remote(variable)
        else:
            raise DataAccessError('Unknown uri scheme "%s"' % (scheme,))

        return data_file
