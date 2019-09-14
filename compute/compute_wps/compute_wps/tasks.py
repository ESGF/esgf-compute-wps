import datetime
import os
import sys

import django
from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from django.conf import settings
from django.core import mail
from django.db.models import aggregates
from django.utils import timezone
from jinja2 import Environment, PackageLoader

# Number of days before a job will expire
EXPIRE = os.environ.get('EXPIRE', 30)

# Number of days before a job will expire to warn user
WARN = os.environ.get('WARN', 10)

EXPIRE_DELTA = datetime.timedelta(days=EXPIRE)

WARN_DELTA = datetime.timedelta(days=EXPIRE-WARN)

EXPIRATION_DELTA = datetime.timedelta(days=WARN)

env = Environment(loader=PackageLoader('compute_wps', 'templates'))

logger = get_task_logger('tasks')

app = Celery('compute_wps')

app.conf.task_routes = {
    'compute_wps.tasks.expire_files': {
        'queue': 'periodic',
    },
}

sys.path.insert(0, '/compute/')

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'compute.settings')

django.setup()

from compute_wps import models  # noqa E402


def datetime_format(value, format='%A, %d %b %y'):
    return value.strftime(format)


def filename(value):
    return value.split('/')[-1]


env.filters['datetime'] = datetime_format
env.filters['filename'] = filename


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # sender.add_periodic_task(crontab(), expire_jobs.s())
    pass


@app.task
def check_expired_jobs():
    warn_date = timezone.now() - WARN_DELTA

    annotated = models.Job.objects.annotate(updated_date=aggregates.Max('status__updated_date'))

    almost = annotated.filter(updated_date__lte=warn_date, expired=False)

    logger.info('Found %r jobs that will expire in %r days', almost.count(), WARN)

    group_by_user = {}

    for x in almost:
        if x.user.email in group_by_user:
            group_by_user[x.user.email]['files'].extend([y.path for y in x.output.all()])
        else:
            group_by_user[x.user.email] = {
                'user': x.user,
                'days_until_expire': WARN,
                'expiration_date': timezone.now() + WARN_DELTA,
                'files': [y.path for y in x.output.all()],
            }

    for x, y in group_by_user.items():
        group_by_user[x]['jobs'] = almost.filter(user=y['user'])

    template = env.get_template('warn_expiration_email.html')

    for x in group_by_user.values():
        logger.info('FILES %r', len(x['files']))

        html_message = template.render(**x)

        user = x['user']

        sent = mail.send_mail('LLNL ESGF Compute', html_message, settings.ADMIN_EMAIL, [user.email],
                              html_message=html_message)

        # If we have successfully delivered email mark jobs for expiration
        if sent == 1:
            print('EXPIRED', x['jobs'].update(expired=True))


@app.task
def remove_expired_jobs():
    expired_date = timezone.now() - EXPIRE_DELTA

    annotated = models.Job.objects.annotate(updated_date=aggregates.Max('status__updated_date'))

    annotated.filter(updated_date__lte=expired_date, expired=True).delete()
