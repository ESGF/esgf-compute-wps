from django.core.management.base import BaseCommand, CommandError

from wps import backends

class Command(BaseCommand):
    help = 'Registers processes'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        for backend in backends.Backend.registry.values():
            backend.initialize()

            backend.populate_processes()
