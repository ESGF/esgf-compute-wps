#! /usr/bin/env python

import os
import sys

import bjoern
from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "compute.settings")

application = get_wsgi_application()

bjoern.run(application, sys.argv[1], int(sys.argv[2]))
