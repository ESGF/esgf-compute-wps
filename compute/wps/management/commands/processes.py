import json

from django.db import IntegrityError
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from wps import backends
from wps import models

class Command(BaseCommand):
    help = 'Register processes'

    def handle(self, *args, **options):
        verbose = options['verbosity'] > 1

        # Get the default/local server
        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            raise CommandError('Default server does not exist')

        for name, backend in backends.Backend.registry.iteritems():
            setting_name = 'WPS_{}_ENABLED'.format(name.upper())

            # Default to false just incase the setting does not exist
            enabled = getattr(settings, setting_name, False)

            self.stdout.write(self.style.SUCCESS('Backend "{}" enabled: {}'.format(name, enabled)))

            if enabled:
                backend.populate_processes()

                for process in backend.processes:
                    try:
                        process = models.Process.objects.create(
                            identifier=process['identifier'],
                            backend=process['backend'],
                            abstract=process['abstract'],
                            metadata=json.dumps(process['metadata']),
                            version='devel')
                    except IntegrityError as e:
                        self.stdout.write(self.style.NOTICE('  Error registering process {}'.format(process['identifier'])))

                        if verbose:
                            self.stdout.write(self.style.ERROR('    {}'.format(e)))
                    else:
                        process.server_set.add(server)

                        self.stdout.write(self.style.SUCCESS('  Registered process {}'.format(process.identifier)))
            else:
                result = models.Process.objects.filter(backend=name).delete()

                self.stdout.write(self.style.SUCCESS('  Removed {} processes'.format(result[1].get('wps.Process', 0))))
