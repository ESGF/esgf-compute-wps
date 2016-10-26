import json
import os
from uuid import uuid4 as uuid

import esgf
from PyOphidia import client
from pywps import config

from wps import logger
from wps.conf import settings
from wps.processes import data_manager
from wps.processes import esgf_operation

class OphidiaAverager(esgf_operation.ESGFOperation):
    def __init__(self):
        super(OphidiaAverager, self).__init__()

    @property
    def title(self):
        return 'Ophidia Average'

    def __call__(self, data_manager, status):
        oph_user = settings.OPH_USER
        oph_pass = settings.OPH_PASSWORD
        oph_host = settings.OPH_HOST
        oph_port = settings.OPH_PORT

        cl = client.Client(oph_user, oph_pass, oph_host, oph_port)

        if not cl.last_response:
            raise esgf.WPSServerError('Could not connect to %s' % (oph_host,))

        try:
            container = self._create_container(cl, data_manager)
        except esgf.WPSServerError:
            container = 'wps'

        import_cube = self._importnc(cl, container)

        status('Imported input "%s"' %
               (self.input()[0].uri,))

        reduce_cube = self._reduce(cl, import_cube)

        status('Reduced across "%s" dimensions' %
               (', '.join(self.parameter('axes').values),))

        output = self._export(cl, reduce_cube, container)

        status('Exported output to %s' % (output,))

        self.set_output(output, '')

    def _export(self, client, cube, container):
        cmd = 'oph_exportnc cube=%s;output_path=%s;output_name=%s;'

        output_path = config.getConfigValue('server', 'outputPath', '/var/wps')

        output_name = '%s.nc' % (str(uuid()),)

        client.submit(cmd % (cube,
                             output_path,
                             output_name))

        self._check_status(client)

        output = os.path.join(output_path, output_name)

        logger.debug('Done exporting to "%s"', output)

        return output

    def _reduce(self, client, cube):
        cmd = 'oph_reduce cube=%s;operation=avg;'

        client.submit(cmd % (cube, ))

        self._check_status(client)

        cube = self._parse_cube(client, 'reduce')

        logger.debug('Reduce "avg" finished.')

        return cube

    def _importnc(self, client, container):
        variable = self.input()[0]
        
        cmd = 'oph_importnc container=%s;measure=%s;src_path=%s;'

        client.submit(cmd % (container,
                             variable.var_name,
                             variable.uri))

        self._check_status(client)
    
        cube = self._parse_cube(client, 'importnc')

        logger.debug('Imported "%s" to %s', variable.uri, cube)

        return cube

    def _create_container(self, client, data_manager):
        var = data_manager.metadata(self.input()[0])

        dim = '|'.join([x.id for x in var.getAxisList()])

        imp_dim = '|'.join(self.parameter('axes').values)

        cmd = 'oph_createcontainer container=%s;dim=%s;imp_dim=%s;'

        container = 'wps'

        client.submit(cmd % (container,
                             dim,
                             imp_dim))

        self._check_status(client)

        logger.debug('Created container "%s"', container)

        return container

    def _parse_cube(self, client, key):
        json_doc = self._parse_response(client)

        task = [x for x in json_doc['response'] if x['objkey'] == key]

        cube = [x['message'] for x in task[0]['objcontent'] if 'message' in x]

        return cube[0]

    def _check_status(self, client):
        json_doc = self._parse_response(client)

        status = [x['objcontent'][0]['title']
                  for x in json_doc['response']
                  if x['objkey'] == 'status']

        if not status or status[0].lower() != 'success':
            raise esgf.WPSServerError('Ophidia command "%s" failed.' % (client.last_request,))

    def _parse_response(self, client):
        if not client.last_response:
            raise esgf.WPSServerError('Ophidia command "%s" returned no response.' % (client.last_request,))

        return json.loads(client.last_response)
