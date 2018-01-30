from __future__ import unicode_literals

import ConfigParser
import re

from django import settings
from django.apps import AppConfig

class DjangoConfigParser(ConfigParser.ConfigParser):
    def __init__(self, obj, defaults):
        self.defaults = defaults

        self.obj = obj

        super(DjangoConfigParser, self).__init__()

    @classmethod
    def from_file(cls, obj, filepath):
        config = cls(obj, defaults)

        config.read([filepath])

        return config

    def set_value(self, key, default_value, data_type=str):
        try:
            if data_type == int:
                value = self.getint('default', key)
            elif data_type == float:
                value = self.getfloat('default', key)
            elif data_type == bool:
                value = self.getboolean('default', key)
            else:
                value = self.get('default', key)
                
                matches = re.findall('{.*}', value)

                for match in matches:
                    result = re.match('{(.*)}', match)

                    if result is None or len(result) < 2 or result.group(1) not in self.defaults:
                        continue

                    value = value.replace(match, self.defaults[result.group(1)])
                
        except ConfigParser.NoOptionError:
            value = default_value
        
            pass
    
        prop_key = key.replace('.', '_').upper()

        setattr(self.obj, prop_key, value)  

class WpsConfig(AppConfig):
    name = 'wps'

    def ready(self):
        defaults = {
            'host': settings.HOST,
        }

        config = DjangoConfigParser.from_file(settings, '/etc/config/django.properties', defaults)

        config.set_value('wps.endpoint', 'http://0.0.0.0/wps')
        config.set_value('wps.status_location', 'http://0.0.0.0/wps')
        config.set_value('wps.dap', 'true')
        config.set_value('wps.dap_url', 'http://0.0.0.0/threddsCWT/dodsC/public/{file_name}')
        config.set_value('wps.login_url', 'http://0.0.0.0/wps/home/auth/login/openid')
        config.set_value('wps.profile_url', 'http://0.0.0.0/wps/home/user/profile')
        config.set_value('wps.oauth2.callback', 'http://0.0.0.0/auth/callback')
        config.set_value('wps.openid.trust.root', 'http://0.0.0.0/')
        config.set_value('wps.openid.return.to', 'http://0.0.0.0auth/callback/openid')
        config.set_value('wps.openid.callback.success', 'http://0.0.0.0/wps/home/auth/login/callback')
        config.set_value('wps.password.reset.url', 'http://0.0.0.0/wps/home/auth/reset')
