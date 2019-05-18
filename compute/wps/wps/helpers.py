#! /usr/bin/env python

from __future__ import division
import logging

from wps import WPSError

logger = logging.getLogger('wps.helpers')

DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

def int_or_float(value):
    if isinstance(value, (int, float)):
        return value

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        raise WPSError('Failed to parse "{value}" as a float or int', value=value)
