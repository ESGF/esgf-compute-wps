from django.core.management.base import BaseCommand, CommandError
from django.db import IntegrityError

from wps import models

class Command(BaseCommand):
    help = 'Add computation backend'

    def add_arguments(self, parser):
        parser.add_argument('--add', action='store_true')
        parser.add_argument('--host', type=str)

        parser.add_argument('--remove', action='store_true')
        parser.add_argument('--id', type=int)

    def __get_required_option(self, key, options):
        value = options[key]

        if value == None:
            raise CommandError('Option "{}" is required for action "{}"'.format(key, options['action']))

        return value

    def handle(self, *args, **options):
        fmt = '{}' + '{: ^24}' * 3

        if options['add']:
            host = self.__get_required_option('host', options)

            try:
                models.Server.objects.create(host=host)
            except IntegrityError:
                raise CommandError('Server with hostname "{}" already exists'.format(host))

            self.stdout.write(self.style.SUCCESS('Successfully added backend with host "{}"'.format(host)))
        elif options['remove']:
            id = self.__get_required_option('id', options)

            try:
                server = models.Server.objects.get(pk=id)
            except models.Server.DoesNotExist:
                raise CommandError('Server with ID "{}" does not exist'.format(id))

            if server.host == 'default':
                raise CommandError('Cannot remove the default host')

            server.delete()

            self.stdout.write(self.style.SUCCESS('Successfully removed backend with host "{}"'.format(server.host)))
        else:
            servers = models.Server.objects.all()

            self.stdout.write(self.style.SUCCESS(fmt.format('ID', 'Host', 'Status', 'Added Date')))

            for s in servers:
                self.stdout.write(self.style.SUCCESS(fmt.format(s.id, s.host, s.status, s.added_date.strftime('%x %X'))))
