import contextlib
import hashlib
import logging
import os
import threading
import time

os.environ['CDAT_ANONYMOUS_LOG'] = 'no'

import dask.array as da
import pytest
import requests
import xarray as xr
import zmq
import cdms2
from urllib.parse import urlparse

from compute_tasks import backend

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
    class WorkerWrapper(backend.Worker):
        def run(self, *args, **kwargs):
            self.exc = None
            try:
                super(WorkerWrapper, self).run()
            except Exception as e:
                self.exc = e

    w = WorkerWrapper(b'devel', '127.0.0.1:8787')

    mocker.patch.object(w, 'action')
    mocker.patch.object(w, 'init_api')

    w.start()

    try:
        yield w
    finally:
        w.stop()

        if w.exc is not None:
            raise w.exc


class ESGFDataManager(object):
    def __init__(self, pytestconfig):
        self.data = {
            'tas': {
                'var': 'tas',
                'files': [
                    'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/CMIP/NCAR/CESM2-WACCM/historical/r2i1p1f1/day/tas/gn/v20190227/tas_day_CESM2-WACCM_historical_r2i1p1f1_gn_18500101-18591231.nc',  # noqa: E501
                    'http://esgf-data.ucar.edu/thredds/dodsC/esg_dataroot/CMIP6/CMIP/NCAR/CESM2-WACCM/historical/r2i1p1f1/day/tas/gn/v20190227/tas_day_CESM2-WACCM_historical_r2i1p1f1_gn_18600101-18691231.nc',  # noqa: E501
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

    def to_cdms2(self, name, file_index=0):
        return cdms2.open(self.data[name]['files'][file_index])

    def to_xarray(self, name, file_index=0, chunks=None):
        return xr.open_dataset(self.data[name]['files'][file_index], chunks=chunks)


@pytest.fixture(scope='session')
def esgf_data(pytestconfig):
    return ESGFDataManager(pytestconfig)
