#! /usr/bin/env python

import logging
import os

from myproxy.client import MyProxyClient

from wps import settings

logger = logging.getLogger(__name__)

def get_certificate(username, password, host, port):
    m = MyProxyClient(hostname=host, caCertDir=settings.CA_PATH)

    c = m.logon(username, password, bootstrap=True)

    return c
