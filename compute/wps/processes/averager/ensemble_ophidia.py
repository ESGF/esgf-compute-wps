import json
import os
import uuid

from PyOphidia import client
from pywps import config
import esgf

from wps.processes import data_manager
from wps.processes import esgf_operation
from wps import logger
from wps.conf import settings

class EnsembleAveragerOphidia(esgf_operation.ESGFOperation):
    def __init__(self):
        super(EnsembleAveragerOphidia, self).__init__()

    @property
    def title(self):
        return 'Ophidia Ensemble Averager'

    def oph_submit(self, client, cmd, ignore_resp=False):
        class OphResponseWrapper(object):
            def __init__(self, response):
                self._data = json.loads(response)

            @property
            def success(self):
                status = self._find_by_objkey('status')

                return status[0]['title'].lower() == 'success'

            def datacube(self, key):
                data = self._find_by_objkey(key) 

                return data[0]['message']

            def _find_by_objkey(self, key):
                try:
                    return [x['objcontent'] for x in self._data['response']
                            if x['objkey'] == key][0]
                except IndexError:
                    return None

        client.submit(cmd)

        if ignore_resp:
            return

        if not client.last_response:
            raise esgf.WPSServerError('Ophidia return no response')

        return OphResponseWrapper(client.last_response)

    def __call__(self, operation, auth, status):
        if len(operation.inputs) < 2:
            raise esgf.WPSServerError('Must supply two input variables.')

        oph_host = settings.OPH_HOST
        oph_port = settings.OPH_PORT
        oph_user = settings.OPH_USER
        oph_pass = settings.OPH_PASSWORD

        cl = client.Client(oph_user, oph_pass, oph_host, oph_port)

        input0, input1 = operation.inputs

        dm = data_manager.DataManager()

        var0 = dm.read(input0)

        var1 = dm.read(input1)

        axis_ids = [x.id for x in var0.chunk.getAxisList()]

        dim = '|'.join(axis_ids)

        container = 'wps_%s' % ('_'.join(axis_ids))

        self.oph_submit(cl, 'oph_createcontainer container=%s;dim=%s;' %
                        (container, dim), True)

        status('Created container "%s" with dimensions "%s"' %
               (container, dim))

        resp = self.oph_submit(cl, 'oph_importnc container=%s;measure=%s;'
                               'src_path=%s;' %
                               (container, input0.var_name, input0.uri))

        if not resp.success:
            raise esgf.WPSServerError('Ophidia import operation failed')

        cube0 = resp.datacube('importnc')

        status('Imported "%s" to "%s"' % (input0.uri, cube0))
            
        resp = self.oph_submit(cl, 'oph_importnc container=%s;measure=%s;'
                               'src_path=%s;' %
                               (container, input1.var_name, input1.uri))

        if not resp.success:
            raise esgf.WPSServerError('Ophidia import operation failed')

        cube1 = resp.datacube('importnc')

        status('Imported "%s" to "%s"' % (input1.uri, cube1))

        resp = self.oph_submit(cl, 'oph_intercube cube=%s;cube2=%s;'
                               'operation=sum;output_measure=%s;container=%s;' % 
                               (cube0, cube1, 'tas', container))

        if not resp.success:
            raise esgf.WPSServerError('Ophidia intercube operation failed')

        out_cube = resp.datacube('intercube')

        status('Finished taking the intercube sum.')

        resp = self.oph_submit(cl, 'oph_apply cube=%s;query=oph_mul_scalar'
                               '(\'oph_float\', \'oph_float\', measure, 0.5);'
                               % (out_cube,))

        if not resp.success:
            raise esgf.WPSServerError('Ophidia apply operation failed.')

        avg_cube = resp.datacube('apply')

        status('Finished element wise multiplication by 0.5')

        resp = self.oph_submit(cl, 'oph_merge cube=%s;' % (avg_cube,))

        if not resp.success:
            raise esgf.WPSServerError('Ophidia merge operation failed.')

        merged_cube = resp.datacube('merge')

        status('Finished merging fragments')

        output_path = config.getConfigValue('server', 'outputPath', '/var/wps')
        output_name = str(uuid.uuid4())

        resp = self.oph_submit(cl, 'oph_exportnc cube=%s;output_path=%s;'
                               'output_name=%s;' %
                               (merged_cube, output_path, output_name))

        if not resp.success:
            raise esgf.WPSServerError('Ophidia export operation failed.')

        status('Finished exporting file')

        output_file = os.path.join(output_path, '%s.nc' % (output_name,)) 

        self.set_output(output_file, 'tas')
