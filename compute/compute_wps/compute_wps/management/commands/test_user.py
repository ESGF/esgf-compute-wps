from django.core.management.base import BaseCommand

from compute_wps import models


class Command(BaseCommand):
    help = 'Create a test user'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        user, created = models.User.objects.get_or_create(username='test_user')

        if created:
            models.Auth.objects.create(user=user)

            user.refresh_from_db()

            user.auth.generate_api_key()

        self.stdout.write(self.style.SUCCESS(user.auth.api_key))
