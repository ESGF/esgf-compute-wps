import os
import requests
from urllib.parse import urlparse

import pytest
import dask.array as da

from compute_tasks import managers


class CachedFileManager(managers.FileManager):
    def __init__(self, cache):
        from unittest import mock
        super(CachedFileManager, self).__init__(mock.MagicMock())

        self.cache = cache
        self.cache_path = os.environ.get('CACHE_PATH', '/data/cache')

        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)

    def local_path(self, uri):
        cached_uri = self.cache.get(uri, None)

        if cached_uri is None or not os.path.exists(cached_uri):
            cached_uri = self.localize_file(uri)

        return cached_uri

    def localize_file(self, uri):
        parts = urlparse(uri)

        cached_uri = os.path.join(self.cache_path, parts.path.split('/')[-1])

        with open(cached_uri, 'wb') as outfile:
            response = requests.get(uri, verify=False)

            response.raise_for_status()

            for chunk in response.iter_content(4096):
                outfile.write(chunk)

        os.chmod(cached_uri, 0o777)

        return cached_uri

    def open_file(self, uri):
        cached_uri = self.cache.get(uri, None)

        if cached_uri is None or not os.path.exists(cached_uri):
            cached_uri = self.localize_file(uri)

            self.cache.set(uri, cached_uri)

        return super(CachedFileManager, self).open_file(cached_uri, True)


class ESGFDataManager(object):
    def __init__(self, pytestconfig):
        self.fm = CachedFileManager(pytestconfig.cache)
        self.data = {
            'tas': {
                'var': 'tas',
                'files': [
                    'http://esgf-data.ucar.edu/thredds/fileServer/esg_dataroot/CMIP6/CMIP/NCAR/CESM2-WACCM/historical/r2i1p1f1/day/tas/gn/v20190227/tas_day_CESM2-WACCM_historical_r2i1p1f1_gn_18500101-18591231.nc',  # noqa: E501
                    'http://esgf-data.ucar.edu/thredds/fileServer/esg_dataroot/CMIP6/CMIP/NCAR/CESM2-WACCM/historical/r2i1p1f1/day/tas/gn/v20190227/tas_day_CESM2-WACCM_historical_r2i1p1f1_gn_18600101-18691231.nc',  # noqa: E501
                ],
            },
        }

    def to_input_manager(self, name, domain=None):
        im = managers.InputManager(self.fm, self.data[name]['files'], self.data[name]['var'])

        im.load_variables_and_axes(name)

        im.subset(domain)

        return im

    def to_cdms2(self, name, file_index=0):
        return self.fm.open_file(self.data[name]['files'][file_index])

    def to_cdms2_tv(self, name, file_index=0):
        file_obj = self.to_cdms2(name, file_index)

        var_name = self.data[name]['var']

        return file_obj[var_name]

    def to_dask_array(self, name, file_index=0, chunks='auto'):
        tv = self.to_cdms2_tv(name, file_index)

        return da.from_array(tv, chunks=chunks)

    def to_local_path(self, name, file_index=0):
        return self.fm.local_path(self.data[name]['files'][file_index])


@pytest.fixture(scope='session')
def esgf_data(pytestconfig):
    return ESGFDataManager(pytestconfig)
