from django.core.management.base import BaseCommand, CommandError
from django.db.utils import IntegrityError

from wps import models

class Command(BaseCommand):
    help = 'Register a server'

    def add_arguments(self, parser):
        parser.add_argument('--host', type=str, required=True, help='Server address')

    def handle(self, *args, **options):
        host = options['host']

        try:
            models.Server.objects.create(host=host)
        except IntegrityError:
            self.stdout.write(self.style.ERROR('Failed to add server "{}", already exists'.format(host)))
        else:
            self.stdout.write(self.style.SUCCESS('Successfully added server "{}"'.format(host)))
