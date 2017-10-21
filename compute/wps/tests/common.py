import string
import random

from django import test

from . import helpers
from wps import models

def random_str(n):
    return ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(n))

class CommonTestCase(test.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.user = models.User.objects.create_user('test', 'test@gmail.com', 'test')

            cert = helpers.generate_certificate()

            models.Auth.objects.create(user=cls.user, cert=cert)
        except Exception:
            cls.user = models.User.objects.get(username='test')

        cls.server = models.Server.objects.create(host='default')

        cls.processes = []

        for _ in xrange(20):
            process = models.Process.objects.create(identifier=random_str(10), backend='Local')

            process.server_set.add(cls.server)

            cls.processes.append(process)

        cls.files = []

        for _ in xrange(50):
            name = random_str(10)
            host = random_str(10)
            var = random_str(4)

            file_obj = models.File.objects.create(
                name=name,
                host=host,
                variable=var,
                url=var,
                requested=random.randint(10, 50)
            )

            cls.files.append(file_obj)

    @classmethod
    def tearDownClass(cls):
        pass
