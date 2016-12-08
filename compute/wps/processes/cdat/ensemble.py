import os

import cdms2
import esgf
from pywps import config
import uuid

from wps import logger
from wps.processes import esgf_operation

class CDATEnsemble(esgf_operation.ESGFOperation):
    KNOWN_GRIDS = {
        't42': {
            'func': cdms2.createGaussianGrid,
            'args': [128]
        }
    }

    def __init__(self):
        super(CDATEnsemble, self).__init__()

    @property
    def title(self):
        return 'CDAT Ensemble'

    def __call__(self, data_manager, status):
        src = [data_manager.open(x) for x in self.input()]

        new_file_name = '%s.nc' % (str(uuid.uuid4()),)
        new_file_path = config.getConfigValue('server', 'outputPath', '/var/wps')
        new_file = os.path.join(new_file_path, new_file_name)

        fout = cdms2.open(new_file, 'w')

        target_grid = None

        gridder = self.parameter('gridder', required=False)

        if gridder:
            if isinstance(gridder.grid, esgf.Domain):
                target_grid = self._create_grid_from_domain(gridder.grid)
            elif isinstance(gridder.grid, esgf.Variable):
                with cdms2.open(gridder.grid.uri, 'r') as grid_file:
                    var = grid_file[gridder.grid.var_name]

                    target_grid = var.getGrid()
            elif isinstance(gridder.grid, (str, unicode)):
                try:
                    grid = self.KNOWN_GRIDS[gridder.grid.lower()]
                except KeyError:
                    raise esgf.WPSServerError('Unknown target grid %s' %
                                              (gridder.grid,))

                target_grid = grid['func'](*grid['args'])
            else:
                raise esgf.WPSServerError(
                    'Unknown value passed as target grid %r' % (gridder.grid,))

            logger.debug('Target grid %r', target_grid)

        var_name = self.input()[0].var_name

        start = 0
        end = src[0].cdms_variable.shape[0]
        step = 200

        if self.domain:
            time = self.domain.get_dimension('time')

            if time:
                if time.crs == esgf.Dimension.indices:
                    start = time.start
                    end = time.start or end
                elif time.crs == esgf.Dimension.values:
                    src_time = src[0].cdms_variable.getTime()

                    start, end = src_time.mapInterval((str(time.start),
                                                       str(time.end)))
        num_src = len(src)

        for i in xrange(start, end, step):
            logger.debug('Averaging time slice from %s to %s', i, i+step)

            # Grab data slices
            data = [x[i, i+step] for x in src]

            # Regrid data
            if target_grid:
                loggger.debug('Before regrid %s', ', '.join(x.shape for x in data))
                data = [x.regrid(target_grid,
                                 regridTool=gridder.tool,
                                 regridMethod=gridder.method) for x in data]
                loggger.debug('Before regrid %s', ', '.join(x.shape for x in data))

            data_avg = reduce(lambda x, y: x + y, data)

            data_avg /= num_src

            fout.write(data_avg, id=var_name) 

        fout.close()

        dap_url = self.create_dap_url(new_file_name)

        self.set_output(dap_url, var_name)

    def _creaate_grid_from_domain(self, domain):
        lat = self._find_dimension(domain, ('latitude', 'lat'))

        lon = self._find_dimension(domain, ('longitude', 'lon'))

        if not lat.end or lat.end == lat.start:
            lat_start = lat.start
            lat_n = 1
            lat_delta = lat.step
        else:
            lat_start = lat.start
            lat_n = (lat.end - lat.start) / lat.step
            lat_delta = 0

        if not lon.end or lon.end == lon.start:
            lon_start = lon.start
            lon_n = 1
            lon_delta = lon.step
        else:
            lon_start = lon.start
            lon_n = (lon.end - lon.start) / lon.step
            lon_delta = 0

        return cdms2.createUnifromGrid(lat_start, lat_n, lat_delta,
                                       lon_start, lon_n, lon_delta)
