import os
import re

from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist

from myproxy.client import MyProxyClient
from myproxy.client import MyProxyClientGetError
from myproxy.client import MyProxyClientRetrieveError

from wps import logger

from esgf_auth.conf import settings
from esgf_auth.models import MyProxyClientAuth

class MyProxyClientBackend(object):
    def authenticate(self, username=None, password=None):
        """ Tries to authenticate user using MyProxyClient. """
        if username and password:
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                user = User(username=username)

            user_host_pattern = 'https?://(.*).(gov|net|edu)/?.*/openid/(.*)'

            result = re.match(user_host_pattern, username)

            if not result:
                logger.debug('Using default MyProxyClient endpoint "%s"',
                             settings.MPC_HOSTNAME)

                client = MyProxyClient(hostname=settings.MPC_HOSTNAME,
                                       caCertDir=settings.MPC_CA_CERT_DIR)
            else:
                hostname = '%s.%s' % (result.group(0), result.group(1))

                logger.debug('Using custom MyProxyClient endpoint "%s"',
                             hostname)

                client = MyProxyClient(hostname=hostname,
                                       caCertDir=settings.MPC_CA_CERT_DIR)

            try:
                if not result:
                    proxy_cred = client.logon(username, password, bootstrap=True)
                else:
                    proxy_cred = client.logon(result.group(2), password, bootstrap=True)
            except (MyProxyClientGetError, MyProxyClientRetrieveError) as e:
                raise Exception('MyProxyClient error: %s' % e.message)
            else:
                # Only save user once we've authenticated.
                user.save()
                
                try:
                    auth = MyProxyClientAuth.objects.get(user=user)
                except ObjectDoesNotExist:
                    auth = MyProxyClientAuth()

                auth.encrypt_pem(user, password, ''.join(proxy_cred))
                auth.save()

            return user

        return None

    def get_user(self, user_id):
        """ Get user. """
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
