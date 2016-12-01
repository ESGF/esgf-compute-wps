import uuid

from wps import logger
from wps.conf import settings
from wps.processes import ophidia_operation

class EnsembleAveragerOphidia(ophidia_operation.OphidiaOperation):
    def __init__(self):
        super(EnsembleAveragerOphidia, self).__init__()

    @property
    def title(self):
        return 'Ophidia Ensemble Averager'

    def __call__(self, data_manager, status):
        if len(self.input()) < 2:
            raise esgf.WPSServerError('Must supply two input variables.')

        data_list = self.input()

        metadata = data_manager.metadata(data_list[0])

        axis = [x.id for x in metadata.getAxisList()]

        container = self.createcontainer('wps', '|'.join(axis))

        inp_cubes = []

        domain = None

        if self.domain:
            domain = self.domain

            logger.debug('Applying domain "%s" to inputs', domain)

        force_import = self.parameter_bool('force_import', required=False)

        for data in data_list:
            cube = self.importnc(container,
                                 data.uri,
                                 data.var_name,
                                 domain=domain,
                                 use_cache=not force_import)
            
            inp_cubes.append(cube)

            logger.debug('Imported "%s" into cube "%s"', data.uri, cube)

        sum_cube = self.intercube(inp_cubes[0],
                                  inp_cubes[1],
                                  'sum',
                                  'summation')

        for next_cube in inp_cubes[2:]:
            sum_cube = self.intercube(sum_cube, next_cube, 'sum', 'summation')

        factor = 1.0/len(inp_cubes)

        new_measure = 'ensemble_avg'

        avg_cube = self.apply(sum_cube,
                              query='oph_mul_scalar(\'OPH_FLOAT\', '
                              '\'OPH_FLOAT\', measure, %f)' % (factor,),
                              measure=new_measure)

        filename = str(uuid.uuid4())

        self.exportnc2(avg_cube, settings.OPH_DAP_PATH, filename)

        uri_args = {
            'hostname': settings.OPH_HOST,
            'port': settings.OPH_DAP_PORT,
            'filename': '%s.nc' % (filename, ),
        }

        output_uri = settings.OPH_DAP_PATH_FORMAT.format(**uri_args)

        self.set_output(output_uri, new_measure)
