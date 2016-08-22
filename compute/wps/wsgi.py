import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'wps.settings')
os.environ.setdefault('UVCDAT_ANONYMOUS_LOG', 'false')

application = get_wsgi_application()
