import os

import cdms2
from celery.utils.log import get_task_logger
from distributed.protocol.serialize import register_serialization

logger = get_task_logger('dask_serialize')


def regrid_chunk(data, axes, grid, tool, method):
    # Subset time to just fit, don't care if its the correct range
    axes[0] = axes[0].subAxis(0, data.shape[0], 1)

    var = cdms2.createVariable(data, axes=axes)

    data = var.regrid(grid, regridTool=tool, regridMethod=method)

    return data


def retrieve_chunk(url, var_name, selector, cert):
    from compute_tasks import managers

    old_cwd = None
    temp_dir = None

    if selector is None:
        selector = {}

    try:
        if cert is not None:
            temp_dir, _, _ = managers.InputManager.write_user_certificate(cert)

        try:
            if temp_dir is not None:
                old_cwd = os.getcwd()

                os.chdir(temp_dir.name)

            with cdms2.open(url) as infile:
                data = infile(var_name, **selector)
        finally:
            if old_cwd is not None:
                os.chdir(old_cwd)
    finally:
        if temp_dir is not None:
            del temp_dir

    return data


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
