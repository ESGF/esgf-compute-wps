import cdms2
import esgf
import os
from pywps import config
import uuid

from wps import logger
from wps.processes import esgf_operation
from wps.processes.esgf_operation import create_from_def

from memory_profiler import profile

class CDATEnsemble(esgf_operation.ESGFOperation):
    def __init__(self):
        super(CDATEnsemble, self).__init__()

    @property
    def title(self):
        return 'CDAT Ensemble'

    def __call__(self, data_manager, status):
        src = [data_manager.metadata(x) for x in self.input()
               if isinstance(x, esgf.Variable)]

        # Find inputs that are operations and execute them appending the
        # input list
        # NOTE this will be moved into workflow at some point
        input_ops = [x for x in self.input() if isinstance(x, esgf.Operation)]

        for inp in input_ops:
            logger.debug('Executing child operation %s', inp.identifier)

            op = create_from_def(inp)

            op.__call__(data_manager, status)

            src.append(data_manager.metadata(op.output))

        # assumed all files are from the same model
        # TODO add some validation
        time = src[0].getTime()

        temporal, spatial = self._gen_domains(src[0].getAxisList(),
                                              time,
                                              self.domain)
        
        logger.debug('Spatial domain %r', spatial)

        start, end, step = self._convert_temporal_domain(time, temporal)

        logger.debug('Temporal domain: start=%s stop=%s step=%s', start, end, step)

        new_file_name = '%s.nc' % (str(uuid.uuid4()),)
        new_file_path = config.getConfigValue('server', 'outputPath', '/var/wps')
        new_file = os.path.join(new_file_path, new_file_name)

        logger.debug('Writing file to %s', new_file)

        fout = cdms2.open(new_file, 'w')

        var_name = self.input()[0].var_name

        denom = len(src)

        for i in xrange(start, end, step):
            logger.debug('Averaging time slice %s', i)

            data = [x(time=time[i], **spatial) for x in src]

            data_sum = reduce(lambda x, y: x + y, data)

            data_avg = data_sum / denom

            fout.write(data_avg, id=var_name) 

        fout.close()

        dap_url = self.create_dap_url(new_file_name)

        self.set_output(dap_url, var_name)

    def _convert_temporal_domain(self, time, domain):
        start = 0
        end = len(time)
        step = 1

        if domain:
            if domain.crs == esgf.Dimension.indices:
                start = domain.start
                end = domain.end
                step = domain.step
            elif domain.crs == esgf.Dimension.values:
                # Convert values in indices
                try:
                    start, end = time.mapInterval((str(domain.start),
                                                   str(domain.end)))
                except Exception as e:
                    raise esgf.WPSServerError(
                        'MapInterval failed for time domain "%r" error "%s"' %
                        (domain, e))

                step = domain.step
            else:
                raise esgf.WPSServerError('Unknown CRS for time domain "%r"' %
                                          (domain,))

        return start, end, step

    def _gen_domains(self, axes, time, domain):
        temporal = None
        spatial = {}

        if domain:
            for d in domain.dimensions:
                is_time = False

                # Check if the dimension is temporal
                for a in axes:
                    if (d.name == 'time' or
                            (d.name == a.id and a.isTime())):
                        is_time = True

                        temporal = d

                        break

                # Process spatial dimensions
                if not is_time:
                    if d.crs == esgf.Dimension.indices:
                        spatial[d.name] = slice(d.start, d.end)
                    elif d.crs == esgf.Dimension.values:
                        spatial[d.name] = (d.start, d.end)
                    else:
                        raise esgf.WPSServerError('Unknown CRS value "%s"' %
                                                  (temporal,))
    
        return temporal, spatial
