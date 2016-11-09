import httplib
import json
import re

# Little hack to use the timeout argument
old_https = httplib.HTTPSConnection

def _new_https_connection(host, port, context):
    return old_https(host, port, timeout=1440, context=context)

httplib.HTTPSConnection = _new_https_connection

from esgf import errors
from PyOphidia import client

from wps import logger
from wps.conf import settings
from wps.processes import esgf_operation

class OphidiaOperation(esgf_operation.ESGFOperation):
    class OphidiaResponseWrapper(object):
        def __init__(self, response, op):
            self._status = self._find_by_key(response, 'status')[0]['title'].lower()

            self._message = None
            
            msg_content = self._find_by_key(response, op)

            if msg_content:
                self._message = msg_content[0]['message']

        @property
        def status(self):
            return self._status

        @property
        def message(self):
            return self._message

        def _find_by_key(self, response, key):
            match = None

            response = json.loads(response)

            for x in response['response']:
                if x['objkey'] == key:
                    match = x['objcontent']

            return match

    def __init__(self):
        super(OphidiaOperation, self).__init__()

        self._client = client.Client(settings.OPH_USER,
                                     settings.OPH_PASSWORD,
                                     settings.OPH_HOST,
                                     settings.OPH_PORT)

    def _submit(self, cmd, ignore_error=False):
        self._client.submit(cmd)

        if not self._client.last_response:
            if ignore_error:
                return None

            raise errors.WPSServerError('Last ophidia cmd "%s" return no '
                                        'response' %
                                        self._client.last_request)

        op = re.match('^oph_(.*) .*$', cmd).group(1) 

        result = self.OphidiaResponseWrapper(self._client.last_response, op)

        if result.status != 'success':
            raise errors.WPSServerError('Ophidia returned a failure status')

        return result

    def exportnc2(self, cube, output_path, output_name):
        cmd = 'oph_exportnc2 cube=%s;output_path=%s;output_name=%s;' % (
            cube,
            output_path,
            output_name)

        result = self._submit(cmd)

        logger.debug(result.status)
        logger.debug(result.message)

    def reduce(self, cube, operation):
        cmd = 'oph_reduce cube=%s;operation=%s;' % (
            cube,
            operation)
        
        result = self._submit(cmd)

        return result.message

    def importnc(self, container, uri, measure, dim):
        cmd = 'oph_importnc container=%s;measure=%s;src_path=%s;imp_dim=%s;ncores=8;' % (
            container,
            measure,
            uri,
            dim)

        result = self._submit(cmd)

        return result.message

    def createcontainer(self, container, dim):
        container_uniq = '%s_%s' % (container, dim.replace('|', '_'),)

        cmd = 'oph_createcontainer container=%s;dim=%s;' % (
            container_uniq, dim)

        self._submit(cmd, True)

        return container_uniq
