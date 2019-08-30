import os
import logging
import urllib
import tempfile
import time

import cdms2
import requests

from compute_tasks import metrics_ as metrics
from compute_tasks import AccessError
from compute_tasks import WPSError

logger = logging.getLogger('compute_tasks.managers.file_manager')


class FileManager(object):
    def __init__(self, context):
        self.context = context

        self.handles = {}

        self.auth = []

        self.temp_dir = None

        self.cert_path = None

        self.cert_data = None

        self.old_dir = None

    def __del__(self):
        if self.old_dir is not None:
            os.chdir(self.old_dir)

            logger.info('Reverted working directory to %r', os.getcwd())

    @staticmethod
    def write_user_certificate(cert):
        temp_dir = tempfile.TemporaryDirectory()

        logger.info('Using temporary directory %s', temp_dir.name)

        cert_path = os.path.join(temp_dir.name, 'cert.pem')

        with open(cert_path, 'w') as outfile:
            outfile.write(cert)

        logger.info('Wrote cert.pem file')

        dodsrc_path = os.path.join(temp_dir.name, '.dodsrc')

        with open(dodsrc_path, 'w') as outfile:
            outfile.write('HTTP.COOKIEJAR={!s}/.dods_cookie\n'.format(temp_dir.name))
            outfile.write('HTTP.SSL.CERTIFICATE={!s}\n'.format(cert_path))
            outfile.write('HTTP.SSL.KEY={!s}\n'.format(cert_path))
            outfile.write('HTTP.SSL.VERIFY=0\n')

        logger.info('Wrote .dodsrc file')

        return temp_dir, cert_path, dodsrc_path

    def load_certificate(self):
        """ Loads a user certificate.

        First the users certificate is checked and refreshed if needed. It's
        then written to disk and the processes current working directory is
        set, allowing calls to NetCDF library to use the certificate.

        Args:
            user: User object.
        """
        if self.cert_path is not None:
            return self.cert_path

        self.cert_data = self.context.user_cert()

        self.old_dir = os.getcwd()

        self.temp_dir, self.cert_path, dodsrc_path = FileManager.write_user_certificate(self.cert_data)

        os.chdir(self.temp_dir.name)

        logger.info('Changed working directory to %r', self.temp_dir.name)

        return self.cert_path

    def requires_cert(self, uri):
        return uri in self.auth

    def open_file(self, uri, skip_access_check=False):
        logger.info('Opening file %r', uri)

        if uri not in self.handles:
            logger.info('File has not been open before')

            if not skip_access_check and not self.check_access(uri):
                cert_path = self.load_certificate()

                if not self.check_access(uri, cert_path):
                    raise WPSError('File {!r} is not accessible, check the OpenDAP service', uri)

                logger.info('File %r requires certificate', uri)

                self.auth.append(uri)

                time.sleep(4)

            logger.info('Current directory %r', os.getcwd())

            try:
                self.handles[uri] = cdms2.open(uri)
            except Exception as e:
                raise AccessError(uri, e)

        return self.handles[uri]

    def get_variable(self, uri, var_name):
        file = self.open_file(uri)

        logger.info('Retieving FileVariable %r', var_name)

        var = file[var_name]

        if var is None:
            raise WPSError('Did not find variable {!r} in {!r}', var_name, uri)

        return var

    def check_access(self, uri, cert=None):
        url = '{!s}.dds'.format(uri)

        logger.info('Checking access with %r', url)

        parts = urllib.parse.urlparse(url)

        try:
            response = requests.get(url, timeout=(10, 30), cert=cert, verify=False)
        except requests.ConnectTimeout:
            logger.exception('Timeout connecting to %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise AccessError(url, 'Timeout connecting to {!r}'.format(parts.hostname))
        except requests.ReadTimeout:
            logger.exception('Timeout reading from %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise AccessError(url, 'Timeout reading from {!r}'.format(parts.hostname))
        except requests.ConnectionError:
            logger.exception('Connection error %r', parts.hostname)

            metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

            raise AccessError(url, 'Connection error to {!r}'.format(parts.hostname))

        if response.status_code == 200:
            return True

        logger.info('Checking url failed with status code %r', response.status_code)

        metrics.WPS_DATA_ACCESS_FAILED.labels(parts.hostname).inc()

        return False
