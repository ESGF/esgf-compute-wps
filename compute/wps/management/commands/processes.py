from django import db
from django.core.management.base import BaseCommand, CommandError

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
            server = models.Server.objects.get(host='default')

            for name, backend in backends.Backend.registry.iteritems():
                self.stdout.write(self.style.SUCCESS('Registering backend {}'.format(name)))

                backend.populate_processes()

                for process in backend.processes:
                    try:
                        process = models.Process.objects.create(**process)
                    except db.IntegrityError:
                        self.stdout.write(self.style.ERROR('  {} already exists'.format(process['identifier'])))
                    else:
                        process.server_set.add(server)

                        self.stdout.write(self.style.SUCCESS('  Registered process {}'.format(process.identifier)))
