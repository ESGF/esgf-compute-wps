from django import db
from django.conf import settings
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
                setting_name = 'WPS_{}_ENABLED'.format(name.upper())

                # Default to false just incase the setting does not exist
                enabled = getattr(settings, setting_name, False)

                # Always have the local backend enabled
                if name == 'Local':
                    enabled = True

                self.stdout.write(self.style.SUCCESS('Backend "{}" enabled: {}'.format(name, enabled)))

                if enabled:
                    backend.populate_processes()

                    for process in backend.processes:
                        if process['identifier'] in settings.PROCESS_BLACKLIST:
                            try:
                                process = models.Process.objects.get(identifier=process['identifier'], backend=name)
                            except models.Process.DoesNotExist:
                                pass
                            else:
                                process.delete() 

                                self.stdout.write(self.style.SUCCESS('  Removed blacklist process "{}"'.format(process.identifier)))
                        else:
                            try:
                                process = models.Process.objects.create(**process)
                            except db.IntegrityError:
                                self.stdout.write(self.style.ERROR('  {} already exists'.format(process['identifier'])))
                            else:
                                process.server_set.add(server)

                                self.stdout.write(self.style.SUCCESS('  Registered process {}'.format(process.identifier)))
                else:
                    processes = models.Process.objects.filter(backend=name)

                    if len(processes) > 0:
                        processes.delete()

                        self.stdout.write(self.style.SUCCESS('  Removed {} processes'.format(result[1].get('wps.Process', 0))))
