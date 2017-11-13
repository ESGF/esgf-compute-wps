#! /usr/bin/env python

import random
import string

import cdms2
import numpy as np
from OpenSSL import crypto
from OpenSSL import SSL
from socket import gethostname

def random_str(count):
    return ''.join(random.choice(string.ascii_letters+string.digits) for _ in xrange(count))

random.seed(1987)

longitude = cdms2.createUniformLongitudeAxis(-180.0, 360.0, 1.0)

latitude = cdms2.createUniformLatitudeAxis(-90.0, 180.0, 1.0)

def write_file(file_path, axes, var_name):
    with cdms2.open(file_path, 'w') as outfile:
        outfile.write(np.array([[[random.random() for _ in xrange(len(axes[2]))] for _ in xrange(len(axes[1]))] for _ in xrange(len(axes[0]))]),
                      axes=axes,
                      id=var_name)

def generate_time(units, n):
    time = cdms2.createAxis(np.array([x for x in xrange(n)]))

    time.id = 'time'

    time.designateTime()

    time.units = units

    return time

DEFAULT_NOT_BEFORE = 0
DEFAULT_NOT_AFTER = 10*365*24*60*60

def generate_certificate(not_before=DEFAULT_NOT_BEFORE, not_after=DEFAULT_NOT_AFTER):
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 2048)

    cert = crypto.X509()
    cert.get_subject().C = "US"
    cert.get_subject().ST = "CA"
    cert.get_subject().O = "test"
    cert.get_subject().OU = "test"
    cert.get_subject().CN = gethostname()
    cert.set_serial_number(1000)
    cert.gmtime_adj_notBefore(not_before)
    cert.gmtime_adj_notAfter(not_after)  # 10 years expiry date
    cert.set_issuer(cert.get_subject())  # self-sign this certificate

    cert.set_pubkey(k)
    cert.sign(k, 'sha256')

    cert_text = crypto.dump_certificate(crypto.FILETYPE_PEM, cert)
    key_text = crypto.dump_privatekey(crypto.FILETYPE_PEM, k)

    return ''.join([cert_text, key_text])
