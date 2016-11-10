import httplib
import json
import re

# Little hack to use the timeout argument
old_https = httplib.HTTPSConnection

def _new_https_connection(host, port, context):
    return old_https(host, port, timeout=36000, context=context)

httplib.HTTPSConnection = _new_https_connection

from esgf import errors
from PyOphidia import client

from wps import logger
from wps.conf import settings
from wps.processes import esgf_operation

class OphidiaResponse(object):
    def __init__(self, response):
        self.response = json.loads(response)

        self._status = self.find_by_key('status')[0]['title'].lower()

        if self.status != 'success':
            raise errors.WPSServerError('Ophidia response status="Failure"')

    @property
    def status(self):
        return self._status

    def find_by_key(self, key):
        match = None

        for x in self.response['response']:
            if x['objkey'] == key:
                match = x['objcontent']

        return match

class OphidiaListResponseWrapper(OphidiaResponse):
    def __init__(self, response):
        super(OphidiaListResponseWrapper, self).__init__(response)

        self._entries = self.find_by_key('list')[0]['rowvalues']

    @property
    def entries(self):
        return self._entries

    def _filter_by_index(self, index, value):
        filtered_list = []

        for entry in self._entries:
            if entry[index] == value:
                filtered_list.append(entry)

        return filtered_list

    def filter_by_src(self, src):
        return self._filter_by_index(6, src)

    def filter_by_container(self, container):
        return self._filter_by_index(1, container)

class OphidiaResponseWrapper(OphidiaResponse):
    def __init__(self, response, op):
        super(OphidiaResponseWrapper, self).__init__(response)

        self._message = None
        
        msg_content = self.find_by_key(op)

        if msg_content:
            self._message = msg_content[0]['message']

    @property
    def message(self):
        return self._message

class OphidiaOperation(esgf_operation.ESGFOperation):

    def __init__(self):
        super(OphidiaOperation, self).__init__()

        self._client = client.Client(settings.OPH_USER,
                                     settings.OPH_PASSWORD,
                                     settings.OPH_HOST,
                                     settings.OPH_PORT)

    def _submit_server(self, cmd, ignore_error=False):
        self._client.submit(cmd)

        if not self._client.last_response:
            if ignore_error:
                return None

            raise errors.WPSServerError('Last ophidia cmd "%s" return no '
                                        'response' %
                                        self._client.last_request)

        return True

    def _submit(self, cmd, ignore_error=False):
        if not self._submit_server(cmd, ignore_error):
            return None

        op = re.match('^oph_(.*) .*$', cmd).group(1) 

        result = OphidiaResponseWrapper(self._client.last_response, op)

        return result

    def _submit_list(self, cmd, ignore_error=False):
        self._submit_server(cmd, ignore_error)

        return OphidiaListResponseWrapper(self._client.last_response)

    def list(self, level=0, container=None):
        cmd = 'oph_list level=%s;' % (level,)

        if container:
            cmd += 'container_filter=%s;' % (container,)

        return self._submit_list(cmd)

    def apply(self, cube, query, measure=None):
        cmd = 'oph_apply cube=%s;query=%s;ncores=12;' % (cube,
                                              query)

        if measure:
            cmd += 'measure=%s;' % (measure,)

        result = self._submit(cmd)

        return result.message

    def intercube(self, cube, cube2, operation, output_measure='intercube_op'):
        cmd = ('oph_intercube cube=%s;cube2=%s;operation=%s;'
               'output_measure=%s;' % (cube,
                                       cube2,
                                       operation,
                                       output_measure,))
        
        result = self._submit(cmd)

        return result.message

    def exportnc2(self, cube, output_path, output_name):
        cmd = 'oph_exportnc2 cube=%s;output_path=%s;output_name=%s;' % (
            cube,
            output_path,
            output_name)

        result = self._submit(cmd)

    def reduce(self, cube, operation):
        cmd = 'oph_reduce cube=%s;operation=%s;' % (
            cube,
            operation)
        
        result = self._submit(cmd)

        return result.message

    def importnc(self, container, uri, measure, dim=None):
        cmd = 'oph_importnc container=%s;measure=%s;src_path=%s;' % (
            container, measure, uri)

        if dim:
            cmd += 'imp_dim=%s;' % (dim,)

        result = self._submit(cmd)

        return result.message

    def createcontainer(self, container, dim):
        container_uniq = '%s_%s' % (container, dim.replace('|', '_'),)

        cmd = 'oph_createcontainer container=%s;dim=%s;' % (
            container_uniq, dim)

        self._submit(cmd, True)

        return container_uniq
