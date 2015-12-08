# wps_cwt

WPS API for ESGF Compute Working Team

Notes on SLURM demo:

Requires a working SLURM installation.  Both the wps_cwt and "tmp"
directory for pywps argument files needs to be shared from the client
node to the cluster node where the work will be done.

 * server/wps.cfg should be modified for the local relevant paths, and copied to /etc/pywps.cfg

 * server/wps/settings.py should have local relevant paths set as well.

 * server/processes/slurm_dispatcher.py needs to have OUTPUT_BASE_PATH
  set to correspond with the .cfg file above (until this can be
  reconciled)

  
It should be fairly straightforward to add new compute operations for
  SLURM.  Each operation is a file in the server/slurm_ops
  subdirectory.  The filename <x>.py contains the operation named <x>.
  To follow the framework for pywps, each operation should have an
  execute function.  this function takes three parameters: data json
  file (from wps), region/domain json file, path to output (.nc) file.
