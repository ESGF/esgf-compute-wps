import argparse

import cwt
import cdms2

parser = argparse.ArgumentParser()

parser.add_argument('url', action='store', type=str)
parser.add_argument('api_key', action='store', type=str)

args = parser.parse_args()

var_name = 'tas'

files = [
    'http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NOAA/output/NOAA-NCEP/CFSv2-2011/decadal1980/3hr/atmos/tas/r1i1p1/tas_3hr_CFSv2-2011_decadal1980_r1i1p1_198011010300-198101010000.nc',
    'http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NOAA/output/NOAA-NCEP/CFSv2-2011/decadal1980/3hr/atmos/tas/r1i1p1/tas_3hr_CFSv2-2011_decadal1980_r1i1p1_198101010300-198201010000.nc',
]

variables = [cwt.Variable(x, var_name) for x in files]

def test_operation(client, name, inputs, domain=None, gridder=None, **kwargs):
    proc = client.processes(name)[0]

    params = kwargs

    if domain is not None:
        params['domain'] = domain

    if gridder is not None:
        params['gridder'] = gridder

    client.execute(proc, inputs=inputs, **params)

    proc.wait()

    return proc.output

def validate_output(variable, shape):
    with cdms2.open(variable.uri) as infile:
        v = infile[variable.var_name]

        if v.shape == shape:
            print 'VERIFIED {!r} matches {!r}'.format(v.shape, shape)
        else:
            print 'FAILED {!r} does not match {!r}'.format(v.shape, shape)

client = cwt.WPSClient(args.url, api_key=args.api_key, verify=False)

for x in client.processes():
    print x.identifier

domain = cwt.Domain(time=(50, 150), lat=(0, 90))

gridder = cwt.Gridder(grid='gaussian~32')

validate_output(test_operation(client, 'CDAT.aggregate', variables, domain),
                (801, 95, 384))

validate_output(test_operation(client, 'CDAT.subset', variables[:1], domain),
                (89, 95, 384))

domain2 = cwt.Domain(time=(45, 50))

validate_output(test_operation(client, 'CDAT.regrid', variables[:1], domain2,
                               gridder=gridder), (41, 32, 64))

validate_output(test_operation(client, 'CDAT.max', variables[:1],
                               axes='time'), (190, 384))
