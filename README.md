# wps_cwt
WPS API for ESGF Compute Working Team

Notes on SLURM demo:

Requires a working SLURM installation.  Both the wps_cwt and "tmp"
directory for pywps argument files needs to be shared from the client
node to the cluster node where the work will be done.

server/wps.cfg should be modified for the local relevant paths, and copied to /etc/pywps.cfg
server/wps/settings.py should have local relevant paths set as well.
server/processes/slurm_dispatcher.py needs to have OUTPUT_BASE_PATH set to correspond with the .cfg file above (until this can be reconciled)
