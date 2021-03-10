# Provenance

Provenance is embedded as global attributes in every output netCDF file. These attributes can be used to recreate the output; locally in a container or remotely on the WPS server.

## Attributes

- **provenance.wps_identifier**: The identifier from the original WPS request.
- **provenance.wps_server**: The server and path where the original WPS process was executed.
- **provenance.wps_document**: The execute request document that was submitted to the server.
- **provenance.container_image**: The container where the workflow was executed.

## Example

### Generating provenance document using CWT End-user API.

Using [CWT End-user API](https://github.com/ESGF/esgf-compute-api) you can create a provenance document that would be stored in the output by ESGF WPS.

```python
import cwt

v = cwt.Variable('...', 'tas')
client = cwt.LLNLClient('...')
subset = client.CDAT.subset(v, domain=cwt.Domain(time=slice(10, 20)))

subset.to_document()
```

### Viewing the provenance in a WPS output

This example shows printing the provenance.

```python
import cdms2

f = cdms2.open('...')

print(f.attributes['provenance.wps_identifier'])
print(f.attributes['provenance.wps_server'])
print(f.attributes['provenance.wps_document'])
print(f.attributes['provenance.container_image'])
```

### Execute remote job from provenance

This example shows submitting a WPS request from provenance.

```python
import cwt
import cdms2

f = cdms2.open('...')

wps_server = f.attributes['provenance.wps_server']
wps_document = f.attributes['provenance.wps_document']

client = cwt.WPSClient(wps_server)

proc = client.execute(wps_document)

proc.wait()
```

### Execute local job from provenance

This example shows executing a WPS request locally using a container. The following will
convert the WPS document to the data_inputs format. This is then used inside a container
to reproduce the exact steps used to generate the original. A local Dask cluster is spawned
in the container, this can be configured via CLI.

```python
import cdms2

f = cdms2.open('...')

print(f.attributes['provenance.wps_identifier'])
print(f.attributes['provenance.wps_document'])
print(f.attributes['provenance.container_image'])
```

Using provenance from above convert the `wps_document` to the data_inputs format.

```bash
docker run -it --entrypoint /tini <container_image> cwt-convert-document <wps_document>
```

Using the data_inputs from the previous command we can now execute the job. The outputs
will be written to `${PWD}/output`.

```bash
docker run -it --entrypoint /tini -v ${PWD}/output:/data <container_image> compute-tasks-execute <data_inputs> <wps_identifier>
```
