# flake8: noqa
from contextlib import contextmanager
from contextlib import ExitStack

import cdms2
import cwt
import numpy as np
import pytest


def int_or_float(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def convert_value(value):
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    else:
        return int_or_float(value)


@pytest.fixture
def wps_client(request):
    wps_url = request.config.getoption('--wps-url')

    compute_token = request.config.getoption('--compute-token')

    extra = dict((x[0], convert_value(x[1])) for x in request.config.getoption('--extra'))

    # Wrap the openURL function so we can update the verify flag since its ignored >= 0.18 of owslib
    def openURLWrapper(*args, **kwargs):
        from owslib.util import openURL

        kwargs['verify'] = extra.get('verify', kwargs['verify'])

        return openURL(*args, **kwargs)

    cwt.wps_client.wps.openURL = openURLWrapper

    client = cwt.WPSClient(wps_url, compute_token=compute_token, **extra)

    return client


@pytest.fixture
def esgf_data():
    class ESGFData(object):
        tas1 = cwt.Variable('http://aims3.llnl.gov/thredds/dodsC/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/1pctCO2/r1i1p1f1/Amon/tas/gr/v20190718/tas_Amon_E3SM-1-0_1pctCO2_r1i1p1f1_gr_000101-002512.nc', 'tas')
        tas2 = cwt.Variable('http://aims3.llnl.gov/thredds/dodsC/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/1pctCO2/r1i1p1f1/Amon/tas/gr/v20190718/tas_Amon_E3SM-1-0_1pctCO2_r1i1p1f1_gr_002601-005012.nc', 'tas')

        @contextmanager
        def to_cdms2(self, variable, **kwargs):
            input = cdms2.open(variable.uri)

            try:
                yield input(variable.var_name, **kwargs)
            finally:
                input.close()

    return ESGFData()


def test_workflow(wps_client, esgf_data):
    w = wps_client.process_by_name('CDAT.workflow')

    r = wps_client.process_by_name('CDAT.regrid')

    s = wps_client.process_by_name('CDAT.subset')

    s.add_inputs(esgf_data.tas1)

    r.add_inputs(s)

    r.gridder = cwt.Gridder(grid='gaussian~32')

    wps_client.execute(w, [r])

    w.wait()

    assert len(w.output) == 2


def test_regrid(wps_client, esgf_data):
    p = wps_client.process_by_name('CDAT.regrid')

    p.gridder = cwt.Gridder(grid='gaussian~32')

    wps_client.execute(p, [esgf_data.tas1])

    p.wait()

    with ExitStack() as stack:
        output = stack.enter_context(cdms2.open(p.output.uri))

        truth_data = stack.enter_context(esgf_data.to_cdms2(esgf_data.tas1, time=slice(0, 1)))

        grid = cdms2.createGaussianGrid(32)

        truth = truth_data.regrid(grid, regridTool=p.gridder.tool, regridMethod=p.gridder.method)

        assert np.allclose(output['tas'][0], truth[0])


def test_aggregate(wps_client, esgf_data):
    p = wps_client.process_by_name('CDAT.aggregate')

    wps_client.execute(p, [esgf_data.tas1, esgf_data.tas2])

    p.wait()

    with ExitStack() as stack:
        output = stack.enter_context(cdms2.open(p.output.uri))

        t1 = stack.enter_context(esgf_data.to_cdms2(esgf_data.tas1))

        t2 = stack.enter_context(esgf_data.to_cdms2(esgf_data.tas2))

        truth_shape = (t1.shape[0]+t2.shape[0],) + t1.shape[1:]

        assert output['tas'].shape == truth_shape
        assert np.allclose(output['tas'][0], t1[0])
        assert np.allclose(output['tas'][-1], t2[-1])


def test_subset(wps_client, esgf_data):
    p = wps_client.process_by_name('CDAT.subset')

    wps_client.execute(p, [esgf_data.tas1], cwt.Domain(time=(4000, 6000), lat=(-90, 0)))

    p.wait()

    with ExitStack() as stack:
        output = stack.enter_context(cdms2.open(p.output.uri))

        truth = stack.enter_context(esgf_data.to_cdms2(esgf_data.tas1, time=(4000, 6000), lat=(-90, 0)))

        assert output['tas'].shape == truth.shape

        assert np.allclose(output['tas'], truth)
