from django.core.management.base import BaseCommand, CommandError

from wps import node_manager

class Command(BaseCommand):
    help = 'Generates WPS capabilities document'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        manager = node_manager.NodeManager()

        manager.generate_capabilities()
