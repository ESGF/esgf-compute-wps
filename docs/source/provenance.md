# Provenance

Provenance is embedded as global attributes in every output netCDF file. These attributes can be used to recreate the output; locally in a container or remotely on the WPS server.

## Attributes

- **provenance.wps_server**: The server and path where the original WPS process was executed.
- **provenance.wps_document**: The execute request document that was submitted to the server.
- **provenance.container_image**: The container where the workflow was executed.

## Example

### Viewing the provenance

```python
import cdms2

f = cdms2.open('...')

print(f.attributes['provenance.wps_server'])
print(f.attributes['provenance.wps_document'])
print(f.attributes['provenance.container_image'])
```
