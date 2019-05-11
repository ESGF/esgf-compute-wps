from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import IntegrityError

from compute_tasks.base import discover_processes
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

        for data in discover_processes():
            setting_name = 'WPS_{!s}_ENABLED'.format(data['backend'].upper())

            enabled = getattr(settings, setting_name, False)

            if not enabled:
                continue

            try:
                process = models.Process.objects.create(**data)
            except IntegrityError as e:
                self.stdout.write(self.style.ERROR('Process {!r} already registered'.format(data['identifier'])))

                if verbose:
                    self.stdout.write(self.style.ERROR('Exception: {!s}'.format(e)))
            else:
                process.server_set.add(server)

                self.stdout.write(self.style.SUCCESS('Successfully registered process {!r}'.format(data['identifier'])))
