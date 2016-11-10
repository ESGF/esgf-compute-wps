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

        entries = self.list(level=3, container=container)

        inp_cubes = []

        for data in data_list:
            cubes = entries.filter_by_src(data.uri)

            if not len(cubes):
                cube = self.importnc(container, data.uri, data.var_name)
                
                inp_cubes.append(cube)

                logger.debug('Imported "%s" into cube "%s"', data.uri, cube)
            else:
                # Assume ordered chronologically
                inp_cubes.append(cubes[-1][2])

                logger.debug('"%s" already imported into cube "%s"',
                             data.uri,
                             cubes[-1][2])


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
