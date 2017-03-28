from __future__ import unicode_literals

import os

from django.apps import AppConfig

class WpsConfig(AppConfig):
    name = 'wps'

    def ready(self):
        from wps import node_manager

        manager = node_manager.NodeManager()

        manager.initialize()
