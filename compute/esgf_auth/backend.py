from django.contrib.auth.models import User
from django.core.exceptions import ObjectDoesNotExist

from myproxy.client import MyProxyClient
from myproxy.client import MyProxyClientGetError
from myproxy.client import MyProxyClientRetrieveError

from esgf_auth.conf import settings
from esgf_auth.models import MyProxyClientAuth

import os

class MyProxyClientBackend(object):
    def authenticate(self, username=None, password=None):
        """ Tries to authenticate user using MyProxyClient. """
        if username and password:
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                user = User(username=username)

            client = MyProxyClient(hostname=settings.MPC_HOSTNAME,
                                   caCertDir=settings.MPC_CA_CERT_DIR)

            bootstrap = True

            # if ca certs exist no need to grab them again.
            # TODO possible check validity incase the trust roots are outdated
            if os.path.exists(settings.MPC_CA_CERT_DIR):
                bootstrap = False

            try:
                proxy_cred = client.logon(username, password, bootstrap=bootstrap)
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
