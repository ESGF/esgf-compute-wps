#! /usr/bin/env python

def json_dumps_default(x):
    if isinstance(x, slice):
        return {
            'type': 'slice',
            'start': x.start,
            'stop': x.stop,
            'step': x.step,
        }
    else:
        raise TypeError()

def json_loads_object_hook(x):
    if 'slice' in x:
        data = x['slice']

        return slice(data['start'], data['stop'], data['step'])
