import cdms2
from distributed.protocol.serialize import register_serialization


def regrid_data(data, axes, grid, tool, method):
    import cdms2

    # Subset time to just fit, don't care if its the correct range
    axes[0] = axes[0].subAxis(0, data.shape[0], 1)

    var = cdms2.createVariable(data, axes=axes)

    data = var.regrid(grid, regridTool=tool, regridMethod=method)

    return data


def retrieve_chunk(url, var_name, selector, cert):
    with open('cert.pem', 'w') as outfile:
        outfile.write(cert)

    import os

    cert_path = os.path.join(os.getcwd(), 'cert.pem')

    with open('.dodsrc', 'w') as outfile:
        outfile.write('HTTP.COOKIEJAR=.dods_cookies\n')
        outfile.write('HTTP.SSL.CERTIFICATE={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.KEY={}\n'.format(cert_path))
        outfile.write('HTTP.SSL.VERIFY=0\n')

    import cdms2

    with cdms2.open(url) as infile:
        return infile(var_name, **selector)


def serialize_transient_axis(axis):
    axis_data = axis[:]
    bounds = axis.getBounds()

    header = {
        'id': axis.id,
        'axis': {
            'shape': axis_data.shape,
            'dtype': axis_data.dtype.name,
        },
        'bounds': {
            'shape': bounds.shape,
            'dtype': bounds.dtype.name,
        },
        'units': axis.units
    }
    data = [axis_data.tobytes(), bounds.tobytes()]
    return header, data


def deserialize_transient_axis(header, frames):
    import cdms2
    import numpy as np
    axis_data = np.frombuffer(frames[0], dtype=header['axis']['dtype'])
    axis_data = axis_data.reshape(header['axis']['shape'])

    bounds = np.frombuffer(frames[1], dtype=header['bounds']['dtype'])
    bounds = bounds.reshape(header['bounds']['shape'])

    axis = cdms2.createAxis(axis_data, bounds=bounds, id=header['id'])
    axis.units = header['units']

    return axis


register_serialization(cdms2.axis.TransientAxis, serialize_transient_axis, deserialize_transient_axis)
