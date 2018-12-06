from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db.utils import IntegrityError

import cwt

from wps import models
from wps import backends
from wps.util import wps as wps_util

class Command(BaseCommand):
    help = 'Register processes'

    def add_arguments(self, parser):
        parser.add_argument('--clear', action='store_true', help='Clears capabilities')

        parser.add_argument('--print', action='store_true', help='Print capabilities')

    def handle(self, *args, **options):
        if options['clear']:
            server = models.Server.objects.get(host='default')

            server.capabilities = ''

            server.save()

            self.stdout.write(self.style.SUCCESS('Cleared capabilities'))
        elif options['print']:
            server = models.Server.objects.get(host='default')

            capabilities = cwt.wps.CreateFromDocument(server.capabilities)

            print capabilities.toDOM(bds=cwt.bds).toprettyxml()
        else:
            try:
                server = models.Server.objects.get(host='default')
            except models.Server.DoesNotExist:
                self.stdout.write(self.style.ERROR('Failed to set capabilities for default server, does not exist'))
            else:
                processes = []
                backend_cache = {}

                for process in models.Process.objects.all():
                    if process.backend not in backend_cache:
                        backend = backends.Backend.get_backend(process.backend)
                        
                        backend.populate_processes()

                        backend_cache[process.backend] = backend
                    else:
                        backend = backend_cache[process.backend]

                    proc_func = backend.get_process(process.identifier)

                    if proc_func['hidden']:
                        continue

                    proc = cwt.wps.process(process.identifier, process.identifier, '1.0.0')

                    processes.append(proc)

                process_offerings = cwt.wps.process_offerings(processes)

                server.capabilities = wps_util.generate_capabilities(settings, process_offerings)

                server.save()

                self.stdout.write(self.style.SUCCESS('Successfully set the capabilities for the default server'))
