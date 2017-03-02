#! /usr/bin/env python

import logging
import os

from wps import tasks

logger = logging.getLogger(__name__)

class NodeManager(object):

    def initialize(self):
        logger.info('Initializing node manager')
