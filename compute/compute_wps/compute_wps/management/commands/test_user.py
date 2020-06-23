from django.core.management.base import BaseCommand

from compute_wps import models


class Command(BaseCommand):
    help = 'Create a test user'

    def add_arguments(self, parser):
        parser.add_argument('--api-key', type=str, help='Set api_key')

    def handle(self, *args, **options):
        user, created = models.User.objects.get_or_create(username='test_user')

        if created:
            models.Auth.objects.create(user=user)

            user.refresh_from_db()

            if options['api_key'] is not None:
                user.auth.api_key = options['api_key']

                user.auth.save()
            else:
                user.auth.generate_api_key()

        self.stdout.write(self.style.SUCCESS(user.auth.api_key))
