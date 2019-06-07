from django.contrib.auth.models import Permission
from django.core.management.base import BaseCommand

from compute_wps import models


PERMISSIONS = (
    'Can add user file',
    'Can add user process',
    'Can add process',
    'Can add file',
    'Can add message',
    'Can add status',
    'Can change status',
)


class Command(BaseCommand):
    help = 'Create an API user'

    def add_arguments(self, parser):
        parser.add_argument('username')

        parser.add_argument('password')

    def handle(self, *args, **options):
        user, created = models.User.objects.get_or_create(username=options['username'])

        permissions = Permission.objects.filter(name__in=PERMISSIONS)

        if created:
            user.set_password(options['password'])

            user.save()

            user.user_permissions.add(*permissions)

            self.stdout.write(self.style.SUCCESS('Create user and set permission'))
        else:
            user.user_permissions.add(*permissions)

            self.stdout.write(self.style.NOTICE('User already exists'))
