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


def int_or_str(value):
    try:
        return int(value)
    except ValueError:
        return value


logger = get_task_logger(__name__)

# Number of days before a job will expire
EXPIRE = float(os.environ.get('JOB_EXPIRATION', 30))

# Number of days before a job will expire to warn user
WARN = float(os.environ.get('JOB_EXPIRATION_WARN', 10))

app = Celery('compute_wps')

# Setup the task routing
app.conf.task_routes = {
    'compute_wps.tasks.*': {
        'queue': 'periodic',
        'exchange': 'periodic',
        'routing_key': 'periodic',
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


ENV = Environment(loader=PackageLoader('compute_wps', 'templates'))

ENV.filters['datetime'] = datetime_format
ENV.filters['filename'] = filename

TEMPLATE = ENV.get_template('warn_expiration_email.html')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    CRONTAB_HOUR = int_or_str(os.environ.get('CRONTAB_HOUR', 23))

    CRONTAB_MINUTE = int_or_str(os.environ.get('CRONTAB_MINUTE', 0))

    check_task = check_expired_jobs.s()

    sender.add_periodic_task(crontab(hour=CRONTAB_HOUR, minute=CRONTAB_MINUTE), check_task)

    remove_task = remove_expired_jobs.s()

    sender.add_periodic_task(crontab(hour=CRONTAB_HOUR, minute=CRONTAB_MINUTE), remove_task)


def gather_expired_jobs():
    warn_date = timezone.now() - datetime.timedelta(EXPIRE-WARN)

    annotated = models.Job.objects.annotate(updated_date=aggregates.Max('status__updated_date'))

    expired = annotated.filter(updated_date__lte=warn_date, expired=False)

    logger.info(f'Found {expired.count()} jobs that completed before {warn_date}')

    return expired


def group_by_user(expired):
    group_by_user = {}

    for x in expired:
        if x.user.email not in group_by_user:
            group_by_user[x.user.email] = {
                'user': x.user,
                'days_until_expire': WARN,
                'expiration_date': timezone.now() + datetime.timedelta(WARN),
                'jobs': expired.filter(user=x.user),
            }

    logger.info(f'Grouped expired jobs into {len(group_by_user)} groups')

    return group_by_user


def notify_users(groups):
    logger.info('Sending user emails with expiring files')

    for email, data in groups.items():
        html_message = TEMPLATE.render(**data)

        sent = mail.send_mail('LLNL ESGF Compute', html_message, settings.ADMIN_EMAIL, [email], html_message=html_message)

        # If we have successfully delivered email mark jobs for expiration
        if sent == 1:
            result = data['jobs'].update(expired=True)

            logger.info(f'Successfully sent user email, marked {result} jobs as expired')
        else:
            job_ids = ', '.join(x.id for x in data['jobs'])

            logger.error(f'Failed sending expire notification for jobs, {job_ids}')


@app.task
def check_expired_jobs():
    expired = gather_expired_jobs()

    groups = group_by_user(expired)

    notify_users(groups)

@app.task
def remove_expired_jobs():
    """ Removes expird jobs.
    """
    expired_date = timezone.now() - datetime.timedelta(EXPIRE)

    logger.info('Removing jobs started before %s and expired', expired_date)

    annotated = models.Job.objects.annotate(updated_date=aggregates.Max('status__updated_date'))

    result = annotated.filter(updated_date__lte=expired_date, expired=True).delete()

    logger.info('Removed %r jobs that had that expired', result[1].get('compute_wps.Job', 0))

    return result[1].get('compute_wps.Job')
