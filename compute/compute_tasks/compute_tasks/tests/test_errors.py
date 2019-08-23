import compute_tasks


def test_daskcluster_access_error():
    assert str(compute_tasks.DaskClusterAccessError()) == 'Error connecting to dask scheduler'


def test_access_error():
    expected = "Access error 'file:///test.nc': Write Error"

    assert str(compute_tasks.AccessError('file:///test.nc', 'Write Error')) == expected


def test_wps_error():
    assert str(compute_tasks.WPSError('{!s}', 'Error')) == 'Error'
