import configparser
import logging
import os
import pkg_resources

logger = logging.getLogger('settings')

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
        except (configparser.NoOptionError, configparser.NoSectionError):
            value = default

        if conv is not None:
            value = conv(value)

        return value

DJANGO_CONFIG_PATH = pkg_resources.resource_filename(__name__, 'django.properties')

DJANGO_CONFIG_PATH = os.environ.get('DJANGO_CONFIG_PATH', DJANGO_CONFIG_PATH)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SECRET_KEY = os.environ['SECRET_KEY']

DEBUG = 'WPS_DEBUG' in os.environ

config = DjangoConfigParser.from_file(DJANGO_CONFIG_PATH)

# Default values
ALLOWED_HOSTS = config.get_value('default', 'allowed_hosts', ['*'], list)
SESSION_COOKIE_NAME = config.get_value('default', 'session.cookie.name', 'wps_sessionid')
ESGF_SEARCH = config.get_value('default', 'esgf.search', 'esgf-node.llnl.gov')
INTERNAL_API_URL = config.get_value('default', 'internal_api_url')

# Auth values
AUTH_TRAEFIK = config.get_value('auth', 'traefik', False, bool)
AUTH_KEYCLOAK = config.get_value('auth', 'keycloak', False, bool)
AUTH_KEYCLOAK_URL = config.get_value('auth', 'keycloak.url', '')
AUTH_KEYCLOAK_REALM = config.get_value('auth', 'keycloak.realm', '')
AUTH_KEYCLOAK_CLIENT_ID = config.get_value('auth', 'keycloak.client_id', '')
AUTH_KEYCLOAK_CLIENT_SECRET = config.get_value('auth', 'keycloak.client_secret','')
AUTH_KEYCLOAK_REG_ACCESS_TOKEN = config.get_value('auth', 'keycloak.reg_access_token', '')

# Email values
EMAIL_HOST = config.get_value('email', 'host')
EMAIL_PORT = config.get_value('email', 'port')
EMAIL_HOST_PASSWORD = config.get_value('email', 'password', '')
EMAIL_HOST_USER = config.get_value('email', 'user', '')

# WPS values
WPS_TITLE = config.get_value('wps', 'title')
WPS_ABSTRACT = config.get_value('wps', 'abstract')
WPS_KEYWORDS = config.get_value('wps', 'keywords', [], list)
WPS_PROVIDER_NAME = config.get_value('wps', 'provider.name')
WPS_PROVIDER_SITE = config.get_value('wps', 'provider.site')
WPS_CONTACT_NAME = config.get_value('wps', 'contact.name')
WPS_CONTACT_POSITION = config.get_value('wps', 'contact.position')
WPS_CONTACT_PHONE = config.get_value('wps', 'contact.phone')
WPS_ADDRESS_DELIVERY = config.get_value('wps', 'address.delivery')
WPS_ADDRESS_CITY = config.get_value('wps', 'address.city')
WPS_ADDRESS_AREA = config.get_value('wps', 'address.area')
WPS_ADDRESS_POSTAL = config.get_value('wps', 'address.postal')
WPS_ADDRESS_COUNTRY = config.get_value('wps', 'address.country')
WPS_ADDRESS_EMAIL = config.get_value('wps', 'address.email')

# Output values
OUTPUT_FILESERVER_URL = config.get_value('output', 'fileserver.url')
OUTPUT_DODSC_URL = config.get_value('output', 'dodsc.url')
OUTPUT_LOCAL_PATH = config.get_value('output', 'local.path')

# Server values
EXTERNAL_URL = config.get_value('server', 'external.url')
EXTERNAL_WPS_URL = '{!s}/wps/'.format(EXTERNAL_URL)
STATUS_URL = '{!s}/api/status/{{job_id}}/'.format(EXTERNAL_URL)
ADMIN_EMAIL = config.get_value('server', 'admin.email')
CA_PATH = '/tmp/certs'
USER_TEMP_PATH = '/tmp/users'

# External values
JOBS_URL = config.get_value('external', 'jobs.url')

APPEND_SLASH = False

SESSION_SERIALIZER = 'django.contrib.sessions.serializers.PickleSerializer'

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.filebased.FileBasedCache',
        'LOCATION': '/tmp/django',
    }
}

INSTALLED_APPS = [
    'compute_wps.apps.WpsConfig',
    'rest_framework',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
]

REST_FRAMEWORK = {
    'DEFAULT_RENDERER_CLASSES': (
        'rest_framework.renderers.JSONRenderer',
    ),
    'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema',
    'DEFAULT_AUTHENTICATION_CLASSES': [
        'rest_framework.authentication.BasicAuthentication',
#         'rest_framework.authentication.SessionAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': (
        'rest_framework.permissions.IsAuthenticated',
    ),
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 50,
}

if AUTH_TRAEFIK:
    REST_FRAMEWORK['DEFAULT_AUTHENTICATION_CLASSES'].append(
        'compute_wps.auth.traefik.TraefikAuthentication')

if AUTH_KEYCLOAK:
    REST_FRAMEWORK['DEFAULT_AUTHENTICATION_CLASSES'].append(
        'compute_wps.auth.keycloak.KeyCloakAuthentication')

if DEBUG:
    REST_FRAMEWORK['DEFAULT_RENDERER_CLASSES'] = (
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer'
    )

    INSTALLED_APPS.append('corsheaders')

GRAPPELLI_ADMIN_TITLE = 'ESGF CWT Administration'

try:
    import django_nose  # noqa: F401
except ModuleNotFoundError:
    pass
else:
    INSTALLED_APPS.append('django_nose')

    TEST_RUNNER = 'django_nose.NoseTestSuiteRunner'

    if DEBUG:
        NOSE_ARGS = [
            '--with-coverage',
            '--cover-package=wps.auth,wps.backend,wps.helpers,wps.tasks,wps.views',
        ]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

if not DEBUG:
    MIDDLEWARE.insert(4, 'django.middleware.csrf.CsrfViewMiddleware')

if DEBUG:
    MIDDLEWARE.insert(4, 'corsheaders.middleware.CorsMiddleware')

    CORS_ORIGIN_ALLOW_ALL = True

    CORS_ALLOW_CREDENTIALS = True

    SESSION_COOKIE_DOMAIN = None

ROOT_URLCONF = 'compute_wps.urls'

WSGI_APPLICATION = 'compute_wps.wsgi.application'

DATABASES = {}

if DEBUG:
    DATABASES['default'] = {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    }
else:
    DATABASES['default'] = {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.getenv('POSTGRES_NAME', 'postgres'),
        'USER': os.getenv('POSTGRES_USER', 'postgres'),
        'PASSWORD': os.getenv('POSTGRES_PASSWORD', '1234'),
        'HOST': os.getenv('POSTGRES_HOST', 'localhost'),
    }

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'wps', 'webapp', 'src'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    }
]

AUTHENTICATION_BACKENDS = [
    'django.contrib.auth.backends.ModelBackend',
    'compute_wps.auth.keycloak.KeyCloakAuthorizationCode',
]

# Password validation
# https://docs.djangoproject.com/en/1.10/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'America/Los_Angeles'

USE_I18N = True

USE_L10N = True

USE_TZ = True

STATIC_URL = '/static/'

STATIC_ROOT = '/var/www/static'

STATICFILES_DIRS = (
    os.path.join(BASE_DIR, 'assets'),
)

FIXTURE_DIRS = (
    os.path.join(BASE_DIR, 'compute_wps/fixtures'),
)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '[%(levelname)s][%(asctime)s][%(filename)s[%(funcName)s:%(lineno)s]] %(message)s',
        }
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True,
        },
    }
}
