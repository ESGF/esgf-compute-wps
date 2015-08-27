# Installing the ESGF CWT WPS SERVER

## Step 1: Installing UV-CDAT (optional)

See: [UV-CDAT instructions](http://uvcdat.llnl.gov/installing.html)

Assuming uvcdat installed in: `/usr/local/uvcdat/latest`

## Step 2: Installing Apache2 (if not already there)

### Pre-Requisites

#### APR

[source](http://mirror.metrocast.net/apache//apr/apr-1.5.2.tar.gz)
```
tar xzvf apr-1.5.2.tar.gz
cd apr
./configure --prefix=/usr/local/wps_cwt/apr
make
make install
```

#### APR-util
[source](http://mirror.metrocast.net/apache//apr/apr-util-1.5.4.tar.gz)

```
tar xzvf apr-util-1.5.4.tar.gz
cd apr-util
./configure --prefix=/usr/local/wps_cwt/apr --with-apr=/usr/local/wps_cwt/apr
make
make install
```

#### PCRE
[source](ftp://ftp.csx.cam.ac.uk/pub/software/programming/pcre/pcre-8.36.tar.gz)

```
tar xzvf pcre-8.36.tar.gz
cd pcre2-10.10
./configure --prefix=/usr/local/wps_cwt/pcre
make
make install
```

### Download

Download page is [here](http://httpd.apache.org/download.cgi#apache24)

We are using: [2.4.16](http://ftp.wayne.edu/apache//httpd/httpd-2.4.16.tar.gz)

### Install

```
cd httpd-2.4.16
./configure --prefix=/usr/local/wps_cwt/apache/2.4.16 --with-apr=/usr/local/wps_cwt/apr --with-apr-util=/usr/local/wps_cwt/apr --with-pcre=/usr/local/wps_cwt/pcre
make
make install
```

## Step 3: mod_wsgi

### Installing

Version: 4.4.13
[source](https://github.com/GrahamDumpleton/mod_wsgi/archive/4.4.13.tar.gz)

```
tar xzvf 4.4.13.tar.gz
cd mod_wsgi-4.4.13
./configure --with-apxs=/usr/local/wps_cwt/apache/2.4.16/bin/apxs --with-python=/usr/local/uvcdat/latest/bin/python
make
make install
```

### Configuring

```
vi /usr/local/wps_cwt/apache/2.4.16/conf/httpd.conf
```

Add this line:

```
LoadModule wsgi_module modules/mod_wsgi.so
```

## Step 4: Django

```
/usr/local/uvcdat/latest/bin/pip install --cert=/export/doutriaux1/cspca.cer django
```
Make sure it work by runnning:

```
python -c "import django; print(django.get_version())"
```

It should output the installed version (1.8.3 at the time we write this)

```
1.8.3
```

## Step 5: Python xtra dependencies

```
/usr/local/uvcdat/latest/bin/pip install --cert=/export/doutriaux1/cspca.cer lxml
/usr/local/uvcdat/latest/bin/pip install --cert=/export/doutriaux1/cspca.cer django-cors-headers
```
`
## Step 5: PyWPS
[source](https://github.com/geopython/PyWPS/archive/pywps-3.2.2.tar.gz)

```
tar xvf pywps-3.2.2.tar.gz
cd PyWPS*
/usr/local/uvcdat/latest/bin/python setup.py install
```

## Step 6: Setting up our server

### Configure the django part

```
cd ~/git/wps_cwt/server
cp wps/settings.template.py wps/settings.py
```

Change the key before going in production mode

```python
SECRET_KEY = 'YOUR KEY HERE'
```

Change the debug to False when going in production

```python
DEBUG = False

TEMPLATE_DEBUG = False
```

Change the path to your templates (full path required by apache)
```python
# Templates
TEMPLATE_DIRS = (
        '/export/doutriaux1/git/wps_cwt/server/templates',
                )
```

The following is needed so your wps process know where to write temporary files

```python
# Where to write temp files
PROCESS_TEMPORARY_FILES = "/tmp"
```

Change the path to your logs

```python

LOGGING = { 
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format' : "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
            'datefmt' : "%d/%b/%Y %H:%M:%S"
        },
    },
    'handlers': {
        'null': {
            'level':'DEBUG',
            'class':'django.utils.log.NullHandler',
        },
        'dj_logfile': {
            'level':'DEBUG',
            'class':'logging.handlers.RotatingFileHandler',
            'filename': "/usr/local/wps_cwt/apache/2.4.16/logs/django.log",
            'maxBytes': 50000,
            'backupCount': 2,
            'formatter': 'standard',
        },
        'console':{
            'level':'INFO',
            'class':'logging.StreamHandler',
            'formatter': 'standard'
        },
    },
    'loggers': {
        'django': {
            'handlers':[ 'dj_logfile' ],
            'propagate': True,
            'level':'DEBUG',
        },
    }
}

```

Start server locally
```
python manage.py runserver
```

point your browser to the [Home Page](http://localhost:8000/)

## Step 7: Deploy in Apache (For production)

### Intro

More detailed info can be found [here](https://docs.djangoproject.com/en/1.8/howto/deployment/wsgi/modwsgi/)

### edit conf file

```
vi /usr/local/wps_cwt/apache/2.4.16/conf/httpd.conf
```

add the following (adapted to your path)

```
# Mod_wsgi for Django
WSGIScriptAlias / /export/doutriaux1/git/wps_cwt/server/wsgi.py
WSGIPythonPath /export/doutriaux1/git/wps_cwt/server

<Directory /export/doutriaux1/git/wps_cwt/server>
<Files wsgi.py>
Require all granted
</Files>
</Directory>

WSGIDaemonProcess aims2.llnl.gov python-path=/export/doutriaux1/git/wps_cwt/server
WSGIProcessGroup aims2.llnl.gov

```



