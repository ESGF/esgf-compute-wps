import random
import string

import cwt
from django.core.management.base import BaseCommand, CommandError

from wps import models

class Command(BaseCommand):
    help = 'Adds fake data'

    def add_arguments(self, parser):
        parser.add_argument('--user', type=int, required=True)
        parser.add_argument('--files', type=int)
        parser.add_argument('--usage', action='store_true')
        parser.add_argument('--process', type=int)
        parser.add_argument('--clear', action='store_true')

    def __get_required_option(self, key, options):
        value = options[key]

        if value == None:
            raise CommandError('Option "{}" is required for action "{}"'.format(key, options['action']))

        return value

    def random_string(self, count):
        return ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(count))

    def random_file_name(self):
        return self.random_string(64) + '.nc'

    def random_file_url(self):
        return 'http://test.com/{}'.format(self.random_file_name())

    def handle(self, *args, **options):
        user = models.User.objects.get(pk=options['user'])

        if options['clear']:
            models.File.objects.all().delete()

            models.Process.objects.all().delete()
        else:
            if options['files'] is not None:
                for _ in xrange(options['files']):
                    var = cwt.Variable(self.random_file_url(), ''.join(random.choice(string.ascii_letters+string.digits)
                                                                       for _ in xrange(random.randint(3, 5))))

                    for _ in xrange(random.randint(5, 10)):
                        models.File.track(user, var)

            if options['process'] is not None:
                for _ in xrange(options['process']):
                    models.Process.objects.create(identifier=self.random_string(10), backend=self.random_string(10))

            if options['usage']:
                for p in models.Process.objects.all():
                    models.UserProcess.objects.create(user=user, process=p, requested=random.randint(10, 60))
