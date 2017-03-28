import httplib
import json
import re
import sys

# Little hack to use the timeout argument
old_https = httplib.HTTPSConnection

def _new_https_connection(host, port, context):
    return old_https(host, port, timeout=36000, context=context)

httplib.HTTPSConnection = _new_https_connection

import esgf
from esgf import errors
from PyOphidia import client
from PyOphidia.client import _ophsubmit
from PyOphidia.client import get_linenumber

from wps import logger
from wps.conf import settings
from wps.processes import esgf_operation

def wsubmit(self, workflow, *params):
    request = None

    try:
        buffer = workflow
        for index, param in enumerate(params, start=1):
            buffer = buffer.replace('${' + str(index) + '}', str(param))
            buffer = re.sub('(\$' + str(index) + ')([^0-9]|$)', str(param) + '\g<2>', buffer)
        request = json.loads(buffer)
    except Exception as e:
        print(get_linenumber(),"Something went wrong in parsing the string:", e)

        return None

    if self.session and 'sessionid' not in request:
        request['sessionid'] = self.session
    if self.cwd and 'cwd' not in request:
        request['cwd'] = self.cwd
    if self.cube and 'cube' not in request:
        request['cube'] = self.cube
    if self.exec_mode and 'exec_mode' not in request:
        request['exec_mode'] = self.exec_mode
    if self.ncores and 'ncores' not in request:
        request['ncores'] = str(self.ncores)

    self.last_request = json.dumps(request)

    try:
        if not self.wisvalid(self.last_request):
            print("The workflow is not valid")
            return None

        self.last_response, self.last_jobid, newsession, return_value, error = _ophsubmit.submit(self.username, self.password, self.server, self.port, self.last_request)

        if return_value:
            raise RuntimeError(error)

        if newsession is not None:
            if len(newsession) == 0:
                self.session = None
            else:
                self.session = newsession
                self.cwd = '/'

        response = self.deserialize_response()

        if response is not None:
            for response_i in response['response']:
                if response_i['objclass'] == 'text':
                    if response_i['objcontent'][0]['title'] == 'Output Cube':
                        self.cube = response_i['objcontent'][0]['message']
                        break

            for response_i in response['response']:
                if response_i['objclass'] == 'text':
                    if response_i['objcontent'][0]['title'] == 'Current Working Directory':
                        self.cwd = response_i['objcontent'][0]['message']
                        break
    except Exception as e:
        print(get_linenumber(),"Something went wrong in submitting the request:", e)
        return None

    return self

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
        for x in self.response['response']:
            if x['objkey'] == key:
                return x['objcontent']

        return None

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

        self._client.wsubmit = wsubmit

    def _submit_server(self, cmd, ignore_error=False):
        cmd += 'ncores=%s;' % (settings.OPH_CORES,)

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

        op = re.match('^oph_([a-z0-9]+) .*$', cmd).group(1) 

        result = OphidiaResponseWrapper(self._client.last_response, op)

        return result

    def _submit_list(self, cmd, ignore_error=False):
        self._submit_server(cmd, ignore_error)

        return OphidiaListResponseWrapper(self._client.last_response)

    def submit_workflow(self, workflow):
        logger.debug(json.dumps(workflow._doc, indent=4))

        self._client.wsubmit(self._client, workflow.to_json)

        logger.debug(json.dumps(self._client.last_response, indent=4))

    def list(self, level=0, **kwargs):
        cmd = 'oph_list level=%s;' % (level,)

        container = kwargs.get('container')

        if container:
            cmd += 'container_filter=%s;' % (container,)

        return self._submit_list(cmd)

    def apply(self, cube, query, measure=None):
        cmd = 'oph_apply cube=%s;query=%s;' % (cube,
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

    def importnc(self, container, uri, measure, **kwargs):
        dim = kwargs.get('dim')
        domain = kwargs.get('domain')
        cache = kwargs.get('use_cache', True)

        if cache:
            logger.debug('Check Ophidias cache')

            entries = self.list(level=3, container=container)

            cubes = entries.filter_by_src(uri)

            if len(cubes):
                logger.debug('Found file already imported')

                return cubes[-1][2]

        cmd = 'oph_importnc container=%s;measure=%s;src_path=%s;' % (
            container, measure, uri)

        if dim:
            cmd += 'imp_dim=%s;' % (dim,)

        if domain:
            cmd += 'subset_dims=%s;' % (
                '|'.join([x.name for x in domain.dimensions]))

            cmd += 'subset_filter=%s;' % (
                '|'.join(['%s:%s' % (x.start, x.end)
                          for x in domain.dimensions]))

            if domain.dimensions[0].crs == esgf.Dimension.indices:
                cmd += 'subset_type=index;'
            else:
                cmd += 'subset_type=coord;'

        result = self._submit(cmd)

        return result.message

    def createcontainer(self, container, dim):
        container_uniq = '%s_%s' % (container, dim.replace('|', '_'),)

        cmd = 'oph_createcontainer container=%s;dim=%s;' % (
            container_uniq, dim)

        self._submit(cmd, True)

        return container_uniq
