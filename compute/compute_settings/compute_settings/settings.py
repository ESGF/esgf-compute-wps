import logging
import os
import configparser

import netaddr

logger = logging.getLogger('wps.settings')

DJANGO_CONFIG_PATH = os.environ.get('DJANGO_CONFIG_PATH', '/etc/config/django.properties')


def patch_settings(settings):
    setattr(settings, 'DEV', 'DEV' in os.environ)

    setattr(settings, 'DEBUG', 'WPS_DEBUG' in os.environ)

    setattr(settings, 'TEST', 'WPS_TEST' in os.environ)

    config = DjangoConfigParser.from_file(DJANGO_CONFIG_PATH)

    host = config.get_value('default', 'host')

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

    # Default values
    setattr(settings, 'INTERNAL_LB', config.get_value('default', 'internal.lb'))
    setattr(settings, 'SESSION_COOKIE_NAME', config.get_value('default', 'session.cookie.name', 'wps_sessionid'))
    setattr(settings, 'ESGF_SEARCH', config.get_value('default', 'esgf.search', 'esgf-node.llnl.gov'))

    # Email values
    setattr(settings, 'EMAIL_HOST', config.get_value('email', 'host'))
    setattr(settings, 'EMAIL_PORT', config.get_value('email', 'port'))
    setattr(settings, 'EMAIL_HOST_PASSWORD', config.get_value('email', 'password', ''))
    setattr(settings, 'EMAIL_HOST_USER', config.get_value('email', 'user', ''))

    # WPS values
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

    # Output values
    setattr(settings, 'OUTPUT_FILESERVER_URL', config.get_value('output', 'fileserver.url'))
    setattr(settings, 'OUTPUT_DODSC_URL', config.get_value('output', 'dodsc.url'))
    setattr(settings, 'OUTPUT_LOCAL_PATH', config.get_value('output', 'local.path'))

    # Server values
    setattr(settings, 'EXTERNAL_URL', config.get_value('server', 'external.url'))
    setattr(settings, 'EXTERNAL_WPS_URL', '{!s}/wps/'.format(settings.EXTERNAL_URL))
    setattr(settings, 'STATUS_URL', '{!s}/api/status/{{job_id}}/'.format(settings.EXTERNAL_URL))
    setattr(settings, 'OAUTH2_CALLBACK_URL', '{!s}/api/oauth2/callback/'.format(settings.EXTERNAL_URL))
    setattr(settings, 'OPENID_TRUST_ROOT_URL', '{!s}/'.format(settings.EXTERNAL_URL))
    setattr(settings, 'OPENID_RETURN_TO_URL', '{!s}/api/openid/callback/'.format(settings.EXTERNAL_URL))
    setattr(settings, 'ADMIN_EMAIL', config.get_value('server', 'admin.email'))
    setattr(settings, 'CA_PATH', '/tmp/certs')
    setattr(settings, 'USER_TEMP_PATH', '/tmp/users')

    # External values
    setattr(settings, 'JOBS_URL', config.get_value('external', 'jobs.url'))
    setattr(settings, 'LOGIN_URL', config.get_value('external', 'login.url'))
    setattr(settings, 'PROFILE_URL', config.get_value('external', 'profile.url'))
    setattr(settings, 'OPENID_CALLBACK_SUCCESS_URL', config.get_value('external', 'openid.callback.success.url'))


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
        except (configparser.NoOptionError, configparser.NoSectionError):
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
