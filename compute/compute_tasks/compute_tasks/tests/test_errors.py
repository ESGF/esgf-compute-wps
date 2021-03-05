import compute_tasks


def test_access_error():
    expected = "Access error 'file:///test.nc': Write Error"

    assert (
        str(compute_tasks.AccessError("file:///test.nc", "Write Error"))
        == expected
    )


def test_wps_error_no_message():
    assert str(compute_tasks.WPSError()) == ""


def test_wps_error():
    assert str(compute_tasks.WPSError("{!s}", "Error")) == "Error"
