from django.core.management.base import BaseCommand, CommandError
from django.db.utils import IntegrityError

from wps import backends
from wps import models

class Command(BaseCommand):
    help = 'Register processes'

    def add_arguments(self, parser):
        parser.add_argument('--list', action='store_true', help='List existing processes')

        parser.add_argument('--register', action='store_true', help='Register processes from backends')

        parser.add_argument('--clear', action='store_true', help='Clears all processes')

    def handle(self, *args, **options):
        if options['clear']:
            models.Process.objects.all().delete()

        if options['list']:
            for backend in models.Process.objects.distinct('backend').values_list('backend', flat=True):
                self.stdout.write(self.style.SUCCESS('Backend: {}'.format(backend)))

                for process in models.Process.objects.filter(backend=backend):
                    servers = ', '.join(process.server_set.all().values_list('host', flat=True))

                    self.stdout.write(self.style.SUCCESS('  Process {}: {}'.format(process.identifier, servers)))
        elif options['register']:
            for name, backend in backends.Backend.registry.iteritems():
                backend.populate_processes()
