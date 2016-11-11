import uuid

from wps import logger
from wps.conf import settings
from wps.processes import ophidia_operation

class OphidiaAverager(ophidia_operation.OphidiaOperation):
    def __init__(self):
        super(OphidiaAverager, self).__init__()

    @property
    def title(self):
        return 'Ophidia Average'

    def __call__(self, data_manager, status):
        data = self.input()[0]

        metadata = data_manager.metadata(data)

        axis = [x.id for x in metadata.getAxisList()]

        container = self.createcontainer('wps', '|'.join(axis))

        logger.debug('Created containers "%s"', container)

        axes = self.parameter('axes')

        imp_dim = '|'.join(axes.values)

        src_cube = self.importnc(container,
                                 data.uri,
                                 data.var_name,
                                 dim=imp_dim)

        logger.debug('Imported "%s" with implicit dimensions "%s"',
                     data.uri, imp_dim)

        avg_cube = self.reduce(src_cube, 'avg')

        logger.debug('Averaged over "%s" dimensions', imp_dim)

        filename = str(uuid.uuid4())

        self.exportnc2(avg_cube, settings.OPH_DAP_PATH, filename)

        logger.debug('Exported "%s" to "%s.nc"',
                     settings.OPH_DAP_PATH,
                     filename)

        uri_args = {
            'hostname': settings.OPH_HOST,
            'port': settings.OPH_DAP_PORT,
            'filename': '%s.nc' % (filename,),
        }

        output_uri = settings.OPH_DAP_PATH_FORMAT.format(**uri_args)

        self.set_output(output_uri, data.var_name)
