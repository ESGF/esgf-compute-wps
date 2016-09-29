from __future__ import unicode_literals

from django.contrib.auth.models import User
from django.db import models

from uuid import uuid4 as uuid
from datetime import datetime

from Crypto.Cipher import AES
from Crypto.Hash import SHA256

from OpenSSL import crypto

class MyProxyClientAuth(models.Model):
    """ Databse table to hold a users MyProxyClient proxy certificats. """

    user = models.OneToOneField(User)
    pem = models.BinaryField()
    salt = models.CharField(max_length=16)

    def get_expiry(self, password):
        """ Get time until certificate expires in seconds. """
        raw_pem = self.decrypt_pem(password)
        
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, raw_pem)

        exp_date = datetime.strptime(cert.get_notAfter(), '%Y%m%d%H%M%SZ')

        exp_delta = exp_date - datetime.now()

        return exp_delta.seconds

    def decrypt_pem(self, password):
        """ Decrypt pem using AES256 with a SHA256 key. """
        hash_algo = SHA256.new()
        hash_algo.update('%s%s' % (password, self.salt))
        key = hash_algo.digest()

        cipher = AES.new(key, AES.MODE_CBC, '0' * 16)
        ciphertext = cipher.decrypt(self.pem)

        return ciphertext[:-ord(ciphertext[-1])]

    def encrypt_pem(self, user, password, pem):
        """ Encrypt pem using AES256 with a SHA256 key. """
        salt = uuid()

        hash_algo = SHA256.new()
        hash_algo.update('%s%s' % (password, salt))
        key = hash_algo.digest()

        length = len(pem)
        padding = 16 - (length % 16)
        pem += chr(padding) *  padding

        cipher = AES.new(key, AES.MODE_CBC, '0' * 16)
        ciphertext = cipher.encrypt(pem)

        self.user = user
        self.pem = ciphertext
        self.salt = salt
        
    class Meta:
        db_table = 'auth_esgf_myproxyclient'
