#!/usr/bin/env python
import os
import sys

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mywps.settings")
    os.environ.setdefault("PYWPS_CFG", "/usr/local/cds/web/WPS/wps_cwt/wps_cds.cfg")
    os.environ.setdefault("UVCDAT_SETUP_PATH", "/Users/thomas/Development/uvcdat/master/build/install")
    os.environ.setdefault("DOCUMENT_ROOT", "/usr/local/cds/web/WPS/wps_cwt/mywps")
    
#export LD_LIBRARY_PATH=/Users/thomas/Development/uvcdat/master/build/install/Externals/lib
#export PYTHONPATH=/Users/thomas/Development/uvcdat/master/build/install/Externals/lib/python2.7/site-packages
#export DYLD_FALLBACK_LIBRARY_PATH=/Users/thomas/Development/uvcdat/master/build/install/Externals/lib

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
