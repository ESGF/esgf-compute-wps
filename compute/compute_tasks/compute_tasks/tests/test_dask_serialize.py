import os

import cdms2
import numpy as np

from compute_tasks import dask_serialize


def test_regrid_chunk(esgf_data):
    tas = esgf_data.to_cdms2('tas')

    grid = cdms2.createGaussianGrid(32)

    output = dask_serialize.regrid_chunk(tas['tas'], tas['tas'].getAxisList(), grid, 'esmf', 'linear')

    assert output.shape == (3650, 32, 64)


def test_retrieve_chunk_certificate(esgf_data, mocker):
    mock_temp_dir = mocker.MagicMock()
    mock_temp_dir.name = '/tmp'

    input_manager = mocker.patch('compute_tasks.managers.InputManager')
    input_manager.write_user_certificate.return_value = (mock_temp_dir, None, None)

    mocker.spy(os, 'chdir')

    tas = esgf_data.to_cdms2('tas')

    data = dask_serialize.retrieve_chunk(tas.id, 'tas', None, 'certificate data')

    assert data.id == 'tas'
    assert data.shape == (3650, 192, 288)

    os.chdir.assert_any_call('/tmp')
    os.chdir.assert_any_call(os.getcwd())


def test_retrieve_chunk_selector(esgf_data):
    tas = esgf_data.to_cdms2('tas')

    selector = {
        'time': slice(10, 20, 2),
    }

    data = dask_serialize.retrieve_chunk(tas.id, 'tas', selector, None)

    assert data.id == 'tas'
    assert data.shape == (5, 192, 288)


def test_retrieve_chunk(esgf_data):
    tas = esgf_data.to_cdms2('tas')

    data = dask_serialize.retrieve_chunk(tas.id, 'tas', None, None)

    assert data.id == 'tas'
    assert data.shape == (3650, 192, 288)


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
