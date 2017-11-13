import json
import random
import string

import cwt
from django.core.management.base import BaseCommand, CommandError

from wps import models

class Command(BaseCommand):
    help = 'Adds fake data to database'

    def add_arguments(self, parser):
        parser.add_argument('--servers', type=int, help='Create fake servers')
        parser.add_argument('--processes', type=int, help='Create fake processes')
        parser.add_argument('--users', type=int, help='Create fake users')
        parser.add_argument('--files', type=int, help='Create fake files')
        parser.add_argument('--caches', type=int, help='Create fake cache files')
        parser.add_argument('--clear', action='store_true', help='Clear database')

    def random_str(self, count):
        return ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(count))

    def random_email(self):
        return '{}@{}.{}'.format(self.random_str(10), self.random_str(5), random.choice(['com', 'org', 'gov', 'net']))

    def random_path(self):
        return '/path/{}.nc'.format(self.random_str(32))

    def random_url(self):
        return 'https://{}.com/thredds/{}.nc'.format(self.random_str(16), self.random_str(32))

    def random_openid_url(self, username):
        return 'https://{}.com/openid/{}'.format(self.random_str(16), username)

    def clear(self):
        total = 0

        todo = [
            models.User,
            models.File,
            models.Cache,
            models.Server,
            models.Process,
        ]

        for m in todo:
            if m == models.User:
                removed, _ = m.objects.filter(username__contains='fake_').delete()
            else:
                removed, _ = m.objects.all().delete()

            total += removed

        self.stdout.write(self.style.SUCCESS('Removed {} records'.format(total)))

    def create_users(self, count):
        for _ in xrange(count):
            username = 'fake_{}'.format(self.random_str(10))

            user = models.User.objects.create_user(username, self.random_email(), username)

            models.Auth.objects.create(user=user, openid_url=self.random_openid_url(username))

        self.stdout.write(self.style.SUCCESS('Created {} users'.format(count)))

    def create_files(self, count):
        variables = [self.random_str(4) for _ in xrange(count/4)]
        hosts = [self.random_str(10) for _ in xrange(count/2)]

        for _ in xrange(count):
            filename = self.random_str(16)

            file_hosts = random.sample(hosts, random.randint(1, len(hosts)))

            for h in file_hosts:
                url = 'https://{}.com/thredds/{}.nc'.format(h, filename)

                models.File.objects.create(name=filename, host=h, variable=random.choice(variables), url=url, requested=random.randint(100, 1000000))

        self.stdout.write(self.style.SUCCESS('Created {} files sample from {} hosts and {} variables'.format(count, len(hosts), len(variables))))

    def create_caches(self, count):
        for _ in xrange(count):
            uid = self.random_str(64)

            models.Cache.objects.create(uid=uid, url=self.random_path(), dimensions='0:365:1', size=random.randint(1000, 2000000))

        self.stdout.write(self.style.SUCCESS('Created {} cached files'.format(count)))

    def create_processes(self, count):
        backends = ['backend1', 'backend2', 'backend3']

        for _ in xrange(count):
            process = models.Process.objects.create(identifier=self.random_str(16), backend=random.choice(backends))

            models.ProcessUsage.objects.create(process=process,
                                               executed=random.randint(0, 1000),
                                               success=random.randint(0, 1000),
                                               failed=random.randint(0, 1000),
                                               retry=random.randint(0, 1000))

        self.stdout.write(self.style.SUCCESS('Created {} processes'.format(count)))

    def create_servers(self, count):
        for _ in xrange(count):
            models.Server.objects.create(host=self.random_str(16), capabilities='capabilities')

        self.stdout.write(self.style.SUCCESS('Created {} servers'.format(count)))

    def create_userfiles(self):
        users = models.User.objects.all()

        files = models.File.objects.all()

        file_count = len(files)

        for user in users:
            sample = random.sample(files, random.randint(1, file_count))

            for sample_file in sample:
                models.UserFile.objects.create(user=user, file=sample_file, requested=random.randint(10, 1000))

            self.stdout.write(self.style.SUCCESS('Associated {} files with user {}'.format(len(sample), user.id)))

        self.stdout.write(self.style.SUCCESS('Finished creating user file associations'))

    def create_userprocesses(self):
        users = models.User.objects.all()

        processes = models.Process.objects.all()

        process_count = len(processes)

        for user in users:
            sample = random.sample(processes, random.randint(1, process_count))

            for sample_process in sample:
                models.UserProcess.objects.create(user=user, process=sample_process, requested=random.randint(10, 1000))

            self.stdout.write(self.style.SUCCESS('Associated {} processes with user {}'.format(len(sample), user.id)))

        self.stdout.write(self.style.SUCCESS('Finished creating user process associations'))

    def create_jobs(self):
        servers = models.Server.objects.all()

        users = models.User.objects.all()

        processes = models.Process.objects.all()

        for user in users:
            job_count = random.randint(2, 50)

            for _ in xrange(job_count):
                job = models.Job.objects.create(server=random.choice(servers),
                                                user=user,
                                                process=random.choice(processes))

                job.accepted()

                job.started()

                steps = random.randint(0, 20)

                for i in xrange(steps):
                    job.update_status('Status step {}'.format(i), i*100/steps)

                end_state = random.randint(0, 2)

                if end_state == 0:
                    job.succeeded(json.dumps({'uri': self.random_url()}))
                elif end_state == 1:
                    job.failed('Job Failed')
                else:
                    job.retry('AccessError')

    def handle(self, *args, **options):
        if options['clear']:
            self.clear()

        if options['files']:
            self.create_files(options['files'])

        if options['caches']:
            self.create_caches(options['caches'])

        if options['processes']:
            self.create_processes(options['processes'])

        if options['servers']:
            self.create_servers(options['servers'])

        if options['users']:
            self.create_users(options['users'])

            self.create_userfiles()

            self.create_userprocesses()

            self.create_jobs()
