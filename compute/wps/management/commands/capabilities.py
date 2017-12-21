from django.core.management.base import BaseCommand, CommandError
from django.db.utils import IntegrityError

from wps import models
from wps import wps_xml

class Command(BaseCommand):
    help = 'Register processes'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        processes = [x for x in models.Process.objects.all()]

        capabilities = wps_xml.capabilities_response(processes)

        try:
            server = models.Server.objects.get(host='default')
        except models.Server.DoesNotExist:
            self.stdout.write(self.style.ERROR('Failed to set capabilities for default server, does not exist'))
        else:
            server.capabilities = capabilities.xml()

            server.save()

            self.stdout.write(self.style.SUCCESS('Successfully set the capabilities for the default server'))
