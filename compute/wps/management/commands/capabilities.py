from django.core.management.base import BaseCommand, CommandError

from wps import models
from wps import wps_xml

class Command(BaseCommand):
    help = 'Generates WPS capabilities document'

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
	server = models.Server.objects.get(host='default')

	processes = server.processes.all()

	print len(processes)

	cap = wps_xml.capabilities_response(add_procs=processes)

	server.capabilities = cap.xml()

	server.save()
