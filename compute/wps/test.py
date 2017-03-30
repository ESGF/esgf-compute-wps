import cwt

wps = cwt.WPS('http://0.0.0.0:8000/wps')

op = wps.get_process('CDSpark.min')

tas = cwt.Variable('file:///data/tas_6h.nc', 'tas')

wps.execute(op, inputs=[tas], axes=['x', 'y'])

import time

print op.status_location

while op.processing:
    print op.status

    time.sleep(1)

print [x.data.value for x in op.output]
