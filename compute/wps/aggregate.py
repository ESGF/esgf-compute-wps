import cwt
import time

wps = cwt.WPS('http://0.0.0.0:8000/wps', api_key='UfHdXPHb8A9o7DRI21pSxNe0Ur6u71IWifUYnoY4dWGvYFqS8P4fU80soR38L9Ud')

files = [
	cwt.Variable('http://esgf-data1.diasjp.net/thredds/dodsC/esg_dataroot/cmip5/output1/MRI/MRI-CGCM3/decadal1965/mon/atmos/Amon/r8i1p1/v20120701/tas/tas_Amon_MRI-CGCM3_decadal1965_r8i1p1_196507-196512.nc', 'tas'),
	cwt.Variable('http://esgf-data1.diasjp.net/thredds/dodsC/esg_dataroot/cmip5/output1/MRI/MRI-CGCM3/decadal1965/mon/atmos/Amon/r8i1p1/v20120701/tas/tas_Amon_MRI-CGCM3_decadal1965_r8i1p1_196601-197512.nc', 'tas'),
]

proc = wps.get_process('CDAT.subset')

domain = cwt.Domain([
	cwt.Dimension('time', '1965-07-01 00:00:00.0', '1975-12-01 00:00:00.0', step=1, crs=cwt.CRS('timestamps')),
	cwt.Dimension('longitude', -180, 180, step=1),
	cwt.Dimension('latitude', 90, -90, step=1),
])

wps.execute(proc, inputs=[files[0]], domain=domain)

while proc.processing:
	print proc.status

	time.sleep(1)

print proc.status

print proc.output.uri

uri = proc.output.localize()

import cdms2

f = cdms2.open(uri)

t = f['tas']

print t.shape
