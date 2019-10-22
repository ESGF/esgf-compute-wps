# flake8: noqa
import os
from contextlib import contextmanager
from contextlib import ExitStack
from functools import partial

os.environ['CDAT_ANONYMOUS_LOG'] = 'yes'

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


def assert_shape(value, expected):
    assert value.shape == expected


@pytest.fixture
def esgf_data(request):
    class ESGFData(object):
        source = {
            'llnl': {
                'tas': {
                    'files': [
                        {
                            'url': 'http://aims3.llnl.gov/thredds/dodsC/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/1pctCO2/r1i1p1f1/Amon/tas/gr/v20190718/tas_Amon_E3SM-1-0_1pctCO2_r1i1p1f1_gr_000101-002512.nc',
                        },
                        {
                            'url': 'http://aims3.llnl.gov/thredds/dodsC/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/1pctCO2/r1i1p1f1/Amon/tas/gr/v20190718/tas_Amon_E3SM-1-0_1pctCO2_r1i1p1f1_gr_002601-005012.nc',
                        }
                    ],
                }
            },
            'ucar': {
                'tas': {
                    'files': [
                        {
                            'url': 'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/LUMIP/NCAR/CESM2/hist-noLu/r1i1p1f1/day/tas/gn/v20190401/tas_day_CESM2_hist-noLu_r1i1p1f1_gn_18500101-18591231.nc',
                        },
                        {
                            'url': 'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/LUMIP/NCAR/CESM2/hist-noLu/r1i1p1f1/day/tas/gn/v20190401/tas_day_CESM2_hist-noLu_r1i1p1f1_gn_18600101-18691231.nc',
                        }
                    ],
                    'subset': {
                        'domain': {
                            'time': (675509.0, 675561.0),
                            'lat': (-90, 0),
                        },
                        'expected': (53, 96, 288),
                    },
                    'aggregate': {
                        'expected': (7300, 192, 288),
                    },
                    'regrid': {
                        'grid': 'gaussian~32',
                        'expected': (3650, 32, 64),
                    },
                    'workflow': {
                        'grid': 'gaussian~32',
                        'axes': 'time',
                        'domain': {
                            'time': (675509.0, 675561.0),
                            'lat': (-90, 0),
                        },
                        'expected': (3650, 32, 64)
                    }
                }
            }
        }

        def __init__(self, request):
            self.request = request

            self._test_data = None

        @property
        def test_data(self):
            if self._test_data is None:
                site = self.request.config.getoption('--site')

                variable = self.request.config.getoption('--variable')

                self._test_data = self.source[site][variable]

            return self._test_data

        def get_workflow(self, site, variable):
            test = self.test_data

            srcs = [cwt.Variable(x['url'], variable) for x in test['files'][:1]]

            domain = cwt.Domain(**test['workflow']['domain'])

            gridder = cwt.Gridder(grid=test['workflow']['grid'])

            return srcs, domain, gridder, test['workflow']['axes'], partial(assert_shape, expected=test['workflow']['expected'])


        def get_regrid(self, site, variable):
            test = self.test_data

            srcs = [cwt.Variable(x['url'], variable) for x in test['files'][:1]]

            return srcs, cwt.Gridder(grid=test['regrid']['grid']), partial(assert_shape, expected=test['regrid']['expected'])

        def get_aggregate(self, site, variable):
            test = self.test_data

            srcs = [cwt.Variable(x['url'], variable) for x in test['files']]

            return srcs, partial(assert_shape, expected=test['aggregate']['expected'])

        def get_subset(self, site, variable):
            test = self.test_data

            srcs = [cwt.Variable(x['url'], variable) for x in test['files'][:1]]

            domain = cwt.Domain(**test['subset']['domain'])

            return srcs, domain, partial(assert_shape, expected=test['subset']['expected'])

        @contextmanager
        def to_cdms2(self, variable, **kwargs):
            input = cdms2.open(variable.uri)

            try:
                yield input(variable.var_name, **kwargs)
            finally:
                input.close()

    return ESGFData(request)


def test_workflow(wps_client, esgf_data):
    w = wps_client.process_by_name('CDAT.workflow')

    r = wps_client.process_by_name('CDAT.regrid')

    s = wps_client.process_by_name('CDAT.subset')

    srcs, domain, gridder, axes, assert_func = esgf_data.get_workflow('ucar', 'tas')

    s.add_inputs(*srcs)

    s.domain = domain

    r.add_inputs(s)

    r.gridder = gridder

    wps_client.execute(w, [r])

    w.wait()

    with ExitStack() as stack:
        output = stack.enter_context(cdms2.open(w.output[0].uri))

        assert_func(output['tas'])


def test_regrid(wps_client, esgf_data):
    p = wps_client.process_by_name('CDAT.regrid')

    srcs, gridder, assert_func = esgf_data.get_regrid('ucar', 'tas')

    p.gridder = gridder

    wps_client.execute(p, srcs)

    p.wait()

    with ExitStack() as stack:
        output = stack.enter_context(cdms2.open(p.output.uri))

        assert_func(output['tas'])


def test_aggregate(wps_client, esgf_data):
    p = wps_client.process_by_name('CDAT.aggregate')

    srcs, assert_func = esgf_data.get_aggregate('ucar', 'tas')

    wps_client.execute(p, srcs)

    p.wait()

    with ExitStack() as stack:
        output = stack.enter_context(cdms2.open(p.output.uri))

        assert_func(output['tas'])


def test_subset(wps_client, esgf_data):
    p = wps_client.process_by_name('CDAT.subset')

    srcs, domain, assert_func = esgf_data.get_subset('ucar', 'tas')

    print(srcs)

    wps_client.execute(p, srcs, domain)

    p.wait()

    with ExitStack() as stack:
        output = stack.enter_context(cdms2.open(p.output.uri))

        assert_func(output['tas'])
