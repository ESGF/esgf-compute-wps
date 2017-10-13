import random
import string

import cwt
from django.core.management.base import BaseCommand, CommandError

from wps import models

class Command(BaseCommand):
    help = 'Adds fake data'

    def add_arguments(self, parser):
        parser.add_argument('--users', type=int)
        parser.add_argument('--files', type=int)
        parser.add_argument('--usage', action='store_true')
        parser.add_argument('--process', type=int)
        parser.add_argument('--clear', action='store_true')

    def __get_required_option(self, key, options):
        value = options[key]

        if value == None:
            raise CommandError('Option "{}" is required for action "{}"'.format(key, options['action']))

        return value

    def random_string_length(self, low, high):
        return ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(random.randint(low, high)))

    def random_string(self, count):
        return ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(count))

    def random_file_name(self):
        return self.random_string(64) + '.nc'

    def random_file_url(self):
        return 'http://test.com/{}'.format(self.random_file_name())

    def handle(self, *args, **options):
        if options['clear']:
            models.User.objects.filter(username__contains='fake').delete()

            models.File.objects.all().delete()

            models.Process.objects.all().delete()

            self.stdout.write(self.style.SUCCESS('Successfully cleared all files and processes'))
        else:
            users = list(models.User.objects.all())

            if options['users'] is not None:
                for i in xrange(options['users']):
                    username = 'fake{}'.format(i)

                    user = models.User.objects.create_user(username, 'fake1@test.com', username)

                    models.Auth.objects.create(user=user)

                    users.append(user)

                    self.stdout.write(self.style.SUCCESS('Created user "{username}" with password "{username}"'.format(username=username)))

                self.stdout.write(self.style.SUCCESS('Successfully created "{}" fake users'.format(options['users'])))
            else:
                user = models.User.objects.create_user('fake1', 'fake1@test.com', 'fake1')

                models.Auth.objects.create(user=user)

                users.append(user)

                self.stdout.write(self.style.SUCCESS('Successfully created fake user "fake1" with password "fake1"'))

            if options['files'] is not None:
                var_name = self.random_string_length(3, 5)

                for _ in xrange(options['files']):
                    # Some entries will have different urls but same variable name
                    if random.random() > 0.5:
                        var_name = self.random_string_length(3, 5)

                    var = cwt.Variable(self.random_file_url(), var_name)

                    user = random.choice(users)

                    # randomize how many times a user has accessed a file
                    for _ in xrange(random.randint(5, 10)):
                        models.File.track(user, var)

                self.stdout.write(self.style.SUCCESS('Successfully created "{}" fake files'.format(options['files'])))

            if options['process'] is not None:
                for _ in xrange(options['process']):
                    models.Process.objects.create(identifier=self.random_string(10), backend=self.random_string(10))

                self.stdout.write(self.style.SUCCESS('Successfully created "{}" fake processes'.format(options['process'])))

            if options['usage']:
                for user in users:
                    for p in models.Process.objects.all():
                        models.UserProcess.objects.create(user=user, process=p, requested=random.randint(10, 60))

                self.stdout.write(self.style.SUCCESS('Successfully created fake process usage'))
