import json

from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from django.db import IntegrityError

from wps import models
from wps.tasks import REGISTRY


class Command(BaseCommand):
    help = 'Register processes'

    def handle(self, *args, **options):
        print(options)
        verbose = options['verbosity'] > 1

        # Get the default/local server
        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            raise CommandError('Default server does not exist')

        for name, process in list(REGISTRY.items()):
            setting_name = 'WPS_{!s}_ENABLED'.format(process.BACKEND.upper())

            enabled = getattr(settings, setting_name, False)

            if enabled:
                self.stdout.write(self.style.WARNING(
                    'Backend {!r} enabled {!s} registering process {!r}'.format(process.BACKEND, enabled, name)))

                try:
                    process = models.Process.objects.create(
                        identifier=name,
                        backend=process.BACKEND,
                        abstract=process.ABSTRACT,
                        metadata=json.dumps(process.METADATA),
                        version='devel')
                except IntegrityError as e:
                    self.stdout.write(self.style.ERROR('Failed to register process {!r}'.format(name)))

                    if verbose:
                        self.stdout.write(self.style.ERROR('Exception: {!s}'.format(e)))
                else:
                    process.server_set.add(server)

                    self.stdout.write(self.style.SUCCESS('Successfully registered process {!r}'.format(name)))
