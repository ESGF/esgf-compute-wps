from django.conf import settings

import os

HOME_DIR = os.path.expanduser('~')
ESG_DIR = os.path.join(HOME_DIR, '.esg')

# MyProxyClient endpoint
MPC_HOSTNAME = getattr(settings, 'MPC_HOSTNAME', 'pcmdi.llnl.gov')
# Directory to hold trusted CA certificates
MPC_CA_CERT_DIR = getattr(settings, 'MPC_CA_CERT_DIR', os.path.join(ESG_DIR, 'certificates'))
# Set session expiration time to be the same as the proxy certificates
MPC_SESSION_EXP = getattr(settings, 'MPC_SESSION_EXP', True)
