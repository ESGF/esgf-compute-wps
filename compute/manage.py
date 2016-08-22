#!/usr/bin/env python
import os
import sys

if __name__ == "__main__":
    project_dir = os.path.dirname( __file__ )
    sys.path.append( project_dir )
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "wps.settings")
    os.environ.setdefault("PYWPS_CFG", os.path.join( project_dir, "wps.cfg" ) )
    os.environ.setdefault("DOCUMENT_ROOT", os.path.join( project_dir, "wps") )
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
