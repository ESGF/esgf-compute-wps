from django.core.management.base import BaseCommand, CommandError
from django.db.utils import IntegrityError

from wps import backends
from wps import models

class Command(BaseCommand):
    help = 'Register processes'

    def add_arguments(self, parser):
        parser.add_argument('--list', action='store_true', help='List existing processes')

        parser.add_argument('--register', action='store_true', help='Register processes from backends')

    def handle(self, *args, **options):
        if options['list']:
            for p in models.Process.objects.order_by('backend', 'identifier'):
                self.stdout.write(self.style.SUCCESS('Process "{}" backend "{}"'.format(p.identifier, p.backend)))
        elif options['register']:
            for name, backend in backends.Backend.registry.iteritems():
                backend.populate_processes()
