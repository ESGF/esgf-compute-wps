#! /usr/bin/env python

import cdms2
import celery
from celery.utils.log import get_task_logger

from wps.processes import cdat

cdms2.setNetcdfShuffleFlag(0)
cdms2.setNetcdfDeflateFlag(0)
cdms2.setNetcdfDeflateLevelFlag(0)

logger = get_task_logger(__name__)

registry = {
    'CDAT.avg': cdat.avg,
    'CDAT.workflow': cdat.workflow,
}

def get_process(identifier):
    try:
        return registry[identifier]
    except KeyError:
        raise Exception('Process "{}" does not exist.'.format(identifier))
