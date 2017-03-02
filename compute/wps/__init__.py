import os

from wps import node_manager

os.environ['UVCDAT_ANONYMOUS_LOG'] = 'no'

default_app_config = 'wps.apps.WpsConfig'

admin = os.getenv('WPS_ADMIN')

if admin is None:
    manager = node_manager.NodeManager()

    manager.initialize()

