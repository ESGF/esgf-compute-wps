import contextlib
import hashlib
import logging
import os
import threading
import time

import dask.array as da
import pytest
import requests
import xarray as xr
import zmq
from urllib.parse import urlparse

from compute_tasks import backend
from compute_tasks import managers

logger = logging.getLogger()


class Provisioner(threading.Thread):
    def __init__(self):
        super(Provisioner, self).__init__(target=self.monitor)

        self.running = True

        self.received = []

        self.workers = []

        self.exc = None

    def stop(self):
        self.running = False

        self.join()

        self.backend.close(0)

        self.context.destroy(0)

    def send(self, worker, frames):
        logger.info('Sending %r', [worker]+frames)

        self.backend.send_multipart([worker]+frames)

    def monitor(self):
        try:
            logger.info('Monitoring')

            self.context = zmq.Context(1)

            self.backend = self.context.socket(zmq.ROUTER)

            backend_addr = 'tcp://*:8787'

            self.backend.bind(backend_addr)

            poller = zmq.Poller()

            poller.register(self.backend, zmq.POLLIN)

            worker = None

            heartbeat_at = time.time() + 1.0

            while self.running:
                socks = dict(poller.poll(1000))

                logger.info('POLL %r', socks)

                if socks.get(self.backend) == zmq.POLLIN:
                    frames = self.backend.recv_multipart()

                    self.received.append(frames)

                    logger.info('Got frames %r %s', frames, type(frames))

                    if frames[1] == b'READY':
                        self.workers.append(frames[0])

                        logger.info('WORKER %s', self.workers)

                if time.time() >= heartbeat_at:
                    logger.info('SENDING heartBEAT')

                    for worker in self.workers:
                        self.backend.send_multipart([worker, b'HEARTBEAT'])

                    heartbeat_at = time.time() + 1.0
        except Exception as e:
            self.exc = e


@pytest.fixture(scope='function')
def provisioner():
    p = Provisioner()

    p.start()

    try:
        yield p
    finally:
        p.stop()

        if p.exc is not None:
            raise p.exc


@pytest.fixture(scope='function')
def worker(mocker):
    w = backend.Worker(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'action')
    mocker.patch.object(w, 'init_api')

    w.start()

    try:
        yield w
    finally:
        w.stop()

        if w.exc is not None:
            raise w.exc


class CachedFileManager(managers.FileManager):
    def __init__(self, cache):
        from unittest import mock
        super(CachedFileManager, self).__init__(mock.MagicMock())

        self.cache = cache
        self.cache_path = os.environ.get('CACHE_PATH', '/data/cache')

        if not os.path.exists(self.cache_path):
            os.makedirs(self.cache_path)

    def local_path(self, uri):
        m = hashlib.sha256(uri.encode())

        return os.path.join(self.cache_path, '{!s}.nc'.format(m.hexdigest()))

    def localize_file(self, uri):
        local_path = self.local_path(uri)

        if not os.path.exists(local_path):
            with open(local_path, 'wb') as outfile:
                response = requests.get(uri, verify=False)

                response.raise_for_status()

                for chunk in response.iter_content(4096):
                    outfile.write(chunk)

            os.chmod(local_path, 0o777)

        return local_path

    def open_file(self, uri):
        local_path = self.localize_file(uri)

        return super(CachedFileManager, self).open_file(local_path, True)


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
            'tas-opendap': {
                'var': 'tas',
                'files': [
                    'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/CMIP/NCAR/CESM2-WACCM/historical/r2i1p1f1/day/tas/gn/v20190227/tas_day_CESM2-WACCM_historical_r2i1p1f1_gn_18500101-18591231.nc',  # noqa: E501
                    'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/CMIP/NCAR/CESM2-WACCM/historical/r2i1p1f1/day/tas/gn/v20190227/tas_day_CESM2-WACCM_historical_r2i1p1f1_gn_18600101-18691231.nc',  # noqa: E501
                ],
            },
            'tas-opendap-cmip5': {
                'var': 'tas',
                'files': [
                    'http://aims3.llnl.gov/thredds/dodsC/cmip5_css02_data/cmip5/output1/CMCC/CMCC-CMS/historical/day/atmos/day/r1i1p1/tas/1/tas_day_CMCC-CMS_historical_r1i1p1_18500101-18591231.nc',  # noqa: E501
                    'http://aims3.llnl.gov/thredds/dodsC/cmip5_css02_data/cmip5/output1/CMCC/CMCC-CMS/historical/day/atmos/day/r1i1p1/tas/1/tas_day_CMCC-CMS_historical_r1i1p1_18600101-18691231.nc',  # noqa: E501
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

    def to_xarray(self, name, file_index=0, chunks=None):
        return xr.open_dataset(self.data[name]['files'][file_index], chunks=chunks)

    def to_local_path(self, name, file_index=0):
        return self.fm.localize_file(self.data[name]['files'][file_index])


@pytest.fixture(scope='session')
def esgf_data(pytestconfig):
    return ESGFDataManager(pytestconfig)
