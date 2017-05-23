#! /usr/bin/env

import os
from contextlib import closing

import cdms2
import numpy as np

def generate_time(start, stop, units, step=1):
    time = cdms2.createAxis(np.array([x for x in xrange(start, stop, step)]))

    time.designateTime()

    time.id = 'time'

    time.units = '{} 0'.format(units)

    return time

def generate_latitude(n, start=-90, delta=1.0):
    return cdms2.createUniformLatitudeAxis(start, n, delta)

def generate_longitude(n, start=0, delta=1.0):
    return cdms2.createUniformLongitudeAxis(start, n, delta)

def generate_variable(value, axes, var_name, identifier):
    file_name = '{}.nc'.format(identifier)

    file_path = '{}/{}'.format(os.path.dirname(__file__), file_name)

    for i, v in enumerate(reversed(axes)):
        if i == 0:
            data = [value for _ in xrange(len(v))]
        else:
            data = [data for _ in xrange(len(v))]

    with closing(cdms2.open(file_path, 'w')) as f:
        f.write(np.array(data), axes=axes, id=var_name)

    return (identifier, {'uri':file_path,'id':'{}|{}'.format(var_name, identifier)})
