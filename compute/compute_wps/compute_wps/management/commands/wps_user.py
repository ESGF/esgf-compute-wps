from django.contrib.auth.models import Permission
from django.core.management.base import BaseCommand

from compute_wps import models


class Command(BaseCommand):
    help = 'Create an admin user'

    def add_arguments(self, parser):
        parser.add_argument('action', choices=('create', 'delete'))

        parser.add_argument('username')


    def handle(self, *args, **options):
        if options['action'] == 'create':
            user, created = models.User.objects.get_or_create(username=options['username'])

            if created:
                models.Auth.objects.create(user=user)

                user.refresh_from_db()

                user.auth.generate_api_key()

                self.stdout.write(self.style.SUCCESS(user.auth.api_key))
        else:
            models.User.objects.get(username=options['username']).delete()

            self.stdout.write(self.style.SUCCESS('Removed user {!r}'.format(options['username'])))
