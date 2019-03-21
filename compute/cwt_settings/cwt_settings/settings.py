from __future__ import division
from future import standard_library
standard_library.install_aliases()
from past.utils import old_div
import datetime
import logging
import os
import configparser

import netaddr

logger = logging.getLogger('wps.settings')

DJANGO_CONFIG_PATH = os.environ.get('DJANGO_CONFIG_PATH', '/etc/config/django.properties')

def patch_settings(settings):
    setattr(settings, 'DEBUG', 'WPS_DEBUG' in os.environ)

    setattr(settings, 'TEST', 'WPS_TEST' in os.environ)

    config = DjangoConfigParser.from_file(DJANGO_CONFIG_PATH)

    host = config.get_value('default', 'host')

    broker_url = config.get_value('default', 'celery.broker')

    result_backend = config.get_value('default', 'celery.backend')

    cidr = config.get_value('default', 'allowed.cidr', None, list)

    # CWT WPS Settings
    if cidr is not None:
        ALLOWED_HOSTS = ['*']

        if netaddr.valid_ipv4(host):
            cidr.append(host)

        cidr = [x for x in cidr if x != '']

        ALLOWED_CIDR_NETS = cidr

        setattr(settings, 'ALLOWED_CIDR_NETS', ALLOWED_CIDR_NETS)
    else:
        ALLOWED_HOSTS = [host]

    setattr(settings, 'ALLOWED_HOSTS', ALLOWED_HOSTS)

    setattr(settings, 'SESSION_COOKIE_NAME', config.get_value('default', 'session.cookie.name', 'wps_sessionid'))

    setattr(settings, 'ACTIVE_USER_THRESHOLD', config.get_value('default', 'active.user.threshold', 5, int, lambda x: datetime.timedelta(days=x)))
    setattr(settings, 'INGRESS_ENABLED', config.get_value('default', 'ingress.enabled', False, bool))
    setattr(settings, 'CERT_DOWNLOAD_ENABLED', config.get_value('default', 'cert.download.enabled', True, bool))
    setattr(settings, 'ESGF_SEARCH', config.get_value('default', 'esgf.search', 'esgf-node.llnl.gov'))

    WORKER_CPU_COUNT = config.get_value('default', 'worker.cpu_count', 2, int)
    WORKER_CPU_UNITS = config.get_value('default', 'worker.cpu_units', 200, int)
    WORKER_USER_PERCENT = config.get_value('default', 'worker.user_percent', 0.10, float)

    setattr(settings, 'WORKER_CPU_COUNT', WORKER_CPU_COUNT)
    setattr(settings, 'WORKER_CPU_UNITS', WORKER_CPU_UNITS)
    setattr(settings, 'WORKER_MEMORY', config.get_value('default', 'worker.memory', 8e6, int))
    setattr(settings, 'WORKER_USER_PERCENT', WORKER_USER_PERCENT)
    setattr(settings, 'WORKER_PER_USER', int((old_div((WORKER_CPU_COUNT*1000),WORKER_CPU_UNITS))*WORKER_USER_PERCENT))

    setattr(settings, 'EMAIL_HOST', config.get_value('email', 'host'))
    setattr(settings, 'EMAIL_PORT', config.get_value('email', 'port'))
    setattr(settings, 'EMAIL_HOST_PASSWORD', config.get_value('email', 'password', ''))
    setattr(settings, 'EMAIL_HOST_USER', config.get_value('email', 'user', ''))

    setattr(settings, 'METRICS_HOST', config.get_value('metrics', 'host'))

    setattr(settings, 'WPS_TITLE', config.get_value('wps', 'title'))
    setattr(settings, 'WPS_ABSTRACT', config.get_value('wps', 'abstract'))
    setattr(settings, 'WPS_KEYWORDS', config.get_value('wps', 'keywords'))
    setattr(settings, 'WPS_PROVIDER_NAME', config.get_value('wps', 'provider.name'))
    setattr(settings, 'WPS_PROVIDER_SITE', config.get_value('wps', 'provider.site'))
    setattr(settings, 'WPS_CONTACT_NAME', config.get_value('wps', 'contact.name'))
    setattr(settings, 'WPS_CONTACT_POSITION', config.get_value('wps', 'contact.position'))
    setattr(settings, 'WPS_CONTACT_PHONE', config.get_value('wps', 'contact.phone'))
    setattr(settings, 'WPS_ADDRESS_DELIVERY', config.get_value('wps', 'address.delivery'))
    setattr(settings, 'WPS_ADDRESS_CITY', config.get_value('wps', 'address.city'))
    setattr(settings, 'WPS_ADDRESS_AREA', config.get_value('wps', 'address.area'))
    setattr(settings, 'WPS_ADDRESS_POSTAL', config.get_value('wps', 'address.postal'))
    setattr(settings, 'WPS_ADDRESS_COUNTRY', config.get_value('wps', 'address.country'))
    setattr(settings, 'WPS_ADDRESS_EMAIL', config.get_value('wps', 'address.email'))

    setattr(settings, 'WPS_URL', config.get_value('wps', 'wps.endpoint'))
    setattr(settings, 'WPS_ENDPOINT', config.get_value('wps', 'wps.endpoint'))
    setattr(settings, 'WPS_STATUS_LOCATION', config.get_value('wps', 'wps.status_location'))
    setattr(settings, 'WPS_JOB_URL', 'https://{!s}/wps/home/user/jobs'.format(host))
    setattr(settings, 'WPS_DAP', True)
    setattr(settings, 'WPS_DAP_URL', config.get_value('wps', 'wps.dap_url'))
    setattr(settings, 'WPS_LOGIN_URL', config.get_value('wps', 'wps.login_url'))
    setattr(settings, 'WPS_PROFILE_URL', config.get_value('wps', 'wps.profile_url'))
    setattr(settings, 'WPS_OAUTH2_CALLBACK', config.get_value('wps', 'wps.oauth2.callback'))
    setattr(settings, 'WPS_OPENID_TRUST_ROOT', config.get_value('wps', 'wps.openid.trust.root'))
    setattr(settings, 'WPS_OPENID_RETURN_TO', config.get_value('wps', 'wps.openid.return.to'))
    setattr(settings, 'WPS_OPENID_CALLBACK_SUCCESS', config.get_value('wps', 'wps.openid.callback.success'))
    setattr(settings, 'WPS_ADMIN_EMAIL', config.get_value('wps', 'wps.admin.email'))

    setattr(settings, 'WPS_INGRESS_PATH', config.get_value('wps', 'wps.ingress_path'))
    setattr(settings, 'WPS_PUBLIC_PATH', config.get_value('wps', 'wps.public_path'))
    setattr(settings, 'WPS_LOCAL_OUTPUT_PATH', config.get_value('wps', 'wps.public_path'))
    setattr(settings, 'WPS_CA_PATH', config.get_value('wps', 'wps.ca.path'))
    setattr(settings, 'WPS_USER_TEMP_PATH', config.get_value('wps', 'wps.user.temp.path'))

    setattr(settings, 'WPS_CACHE_PATH', config.get_value('cache', 'wps.cache.path', '/data/cache'))
    setattr(settings, 'WPS_PARTITION_SIZE', config.get_value('cache', 'wps.partition.size', 10, int))
    setattr(settings, 'WPS_CACHE_CHECK', config.get_value('cache', 'wps.cache.check', 1, int, lambda x: datetime.timedelta(days=x)))
    setattr(settings, 'WPS_CACHE_GB_MAX_SIZE', config.get_value('cache', 'wps.gb.max.size', 2.097152e8, float))
    setattr(settings, 'WPS_CACHE_MAX_AGE', config.get_value('cache', 'wps.cache.max.age', 30, int, lambda x: datetime.timedelta(days=x)))
    setattr(settings, 'WPS_CACHE_FREED_PERCENT', config.get_value('cache', 'wps.cache.freed.percent', 0.25, float))

    setattr(settings, 'WPS_CDAT_ENABLED', config.get_value('wps', 'wps.cdat.enabled', True, bool))

    setattr(settings, 'WPS_EDAS_ENABLED', config.get_value('edas', 'wps.edas.enabled', False, bool))
    setattr(settings, 'WPS_EDAS_HOST', config.get_value('edas', 'wps.edas.host', 'aims2.llnl.gov'))
    setattr(settings, 'WPS_EDAS_REQ_PORT', config.get_value('edas', 'wps.edas.req.port', 5670, int))
    setattr(settings, 'WPS_EDAS_RES_PORT', config.get_value('edas', 'wps.edas.res.port', 5671, int))
    setattr(settings, 'WPS_EDAS_EXECUTE_TIMEOUT', config.get_value('edas', 'timeout.execute', 300, int))
    setattr(settings, 'WPS_EDAS_QUEUE_TIMEOUT', config.get_value('edas', 'timeout.submit', 30, int))
    setattr(settings, 'WPS_EDAS_OUTPUT_PATH', config.get_value('edas', 'output.path', '/data/edask'))

    setattr(settings, 'WPS_OPHIDIA_ENABLED', config.get_value('ophidia', 'wps.oph.enabled', False, bool))
    setattr(settings, 'WPS_OPHIDIA_USER', config.get_value('ophidia', 'wps.oph.user', 'oph-test'))
    setattr(settings, 'WPS_OPHIDIA_PASSWORD', config.get_value('ophidia', 'wps.oph.password', 'abcd'))
    setattr(settings, 'WPS_OPHIDIA_HOST', config.get_value('ophidia', 'wps.oph.host', 'aims2.llnl.gov'))
    setattr(settings, 'WPS_OPHIDIA_PORT', config.get_value('ophidia', 'wps.oph.port', 11732, int))
    setattr(settings, 'WPS_OPHIDIA_OUTPUT_PATH', config.get_value('ophidia', 'wps.oph.output.path', '/wps'))
    setattr(settings, 'WPS_OPHIDIA_OUTPUT_URL', config.get_value('ophidia', 'wps.oph.output.url', 'https://aims2.llnl.gov/thredds/dodsC{output_path}/{output_name}.nc'))
    setattr(settings, 'WPS_OPHIDIA_DEFAULT_CORES', config.get_value('ophidia', 'wps.oph.default.cores', 8, int))

class DjangoConfigParser(configparser.ConfigParser):
    def __init__(self, defaults):
        self.defaults = defaults

        configparser.ConfigParser.__init__(self)

    @classmethod
    def from_file(cls, file_path, defaults=None):
        config = cls(defaults or {})

        config.read([file_path])

        return config

    def get_value(self, section, key, default=None, value_type=str, conv=None):
        try:
            if value_type == int:
                value = self.getint(section, key)
            elif value_type == float:
                value = self.getfloat(section, key)
            elif value_type == bool:
                value = self.getboolean(section, key)
            elif value_type == list:
                value = self.get(section, key).split(',')
            else:
                value = self.get(section, key)

                for replacement in self.defaults.items():
                    if replacement[0] in value:
                        value = value.replace(*replacement)
        # Error with calling NoSectionError
        except TypeError:
            if default is None:
                raise

            value = default

            pass
        except (configparser.NoOptionError, configparser.NoSectionError) as e:
            if default is None:
                raise

            value = default

            if value_type == str:
                for replacement in self.defaults.items():
                    if replacement[0] in value:
                        value = value.replace(*replacement)

            pass

        if conv is not None:
            value = conv(value)

        return value


