import os

import cdms2
import numpy as np

from compute_tasks import dask_serialize


def test_serialize_transient_axis(esgf_data):
    data = esgf_data.to_cdms2('tas')

    axis_index = data['tas'].getAxisIndex('time')

    axis = data['tas'].getAxis(axis_index)

    header, data = dask_serialize.serialize_transient_axis(axis)

    assert header['id'] == 'time'
    assert header['axis']['shape'] == (3650, )
    assert header['axis']['dtype'] == 'float64'
    assert header['bounds']['shape'] == (3650, 2)
    assert header['bounds']['dtype'] == 'float64'
    assert header['units'] == 'days since 0001-01-01 00:00:00'


def test_deserialize_transient_axis(esgf_data):
    data = esgf_data.to_cdms2('tas')

    axis_index = data['tas'].getAxisIndex('time')

    axis = data['tas'].getAxis(axis_index)

    header, frames = dask_serialize.serialize_transient_axis(axis)

    output = dask_serialize.deserialize_transient_axis(header, frames)

    assert axis.id == output.id
    assert axis.shape == output.shape
    assert axis.units == output.units
    assert np.all(np.stack([axis.getBounds(), output.getBounds()]))
