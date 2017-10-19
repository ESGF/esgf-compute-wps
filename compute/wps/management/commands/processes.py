from django.core.management.base import BaseCommand, CommandError

from wps import models
from wps import node_manager

class Command(BaseCommand):
    help = 'Toggles processes'

    def add_arguments(self, parser):
        parser.add_argument('--list', action='store_true')

        parser.add_argument('--enable', type=str, nargs='*')

        parser.add_argument('--disable', type=str, nargs='*')
        
    def handle(self, *args, **options):
        if options['list']:
            self.stdout.write(self.style.SUCCESS('Identifier\tBackend\tEnabled'))

            for proc in models.Process.objects.order_by('identifier'):
                self.stdout.write(self.style.SUCCESS('{}\t{}\t{}'.format(proc.identifier, proc.backend, proc.enabled)))
        else:
            if options['enable'] is not None:
                for identifier in options['enable']:
                    proc = models.Process.objects.get(identifier=identifier)

                    proc.enabled = True
                    
                    proc.save()

            if options['disable'] is not None:
                for identifier in options['disable']:
                    proc = models.Process.objects.get(identifier=identifier)

                    proc.enabled = False

                    proc.save()

            manager = node_manager.NodeManager()

            manager.generate_capabilities()
