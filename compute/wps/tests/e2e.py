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

client = cwt.WPSClient(args.url, api_key=args.api_key, verify=False)

for x in client.processes():
    print x.identifier

def test_operation(client, name, inputs, domain=None, gridder=None, **kwargs):
    proc = client.processes(name)[0]

    params = kwargs

    if domain is not None:
        params['domain'] = domain

    if gridder is not None:
        params['gridder'] = gridder

    client.execute(proc, inputs=inputs, **params)

    proc.wait()

    with cdms2.open(proc.output.uri) as infile:
        v = infile[proc.output.var_name]

        print v.shape

test_operation(client, 'CDAT.aggregate', variables)
