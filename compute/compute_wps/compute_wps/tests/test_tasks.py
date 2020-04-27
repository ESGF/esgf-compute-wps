import datetime
import logging
import os
import random
import re

import pytest
from unittest import mock
from django import test
from django.conf import settings
from django.db.models import aggregates
from django.utils import timezone

from compute_wps import models
from compute_wps import tasks


logger = logging.getLogger(__name__)

def _create_users():
    users = []

    for x in range(4):
        kwargs = {
                'username': f'user-{x}',
                'email': f'user{x}@compute.io',
                'first_name': f'first_name_{x}',
                'last_name': f'last_name_{x}',
                }

        users.append(models.User.objects.create(**kwargs))

    return users

def _create_jobs(users):
    jobs = []

    now = datetime.datetime(2020, 4, 15)

    for x in range(16):
        u = random.choice(users)

        expired = random.choice([True, False])

        if expired:
            marked = random.choice([True, False])
        else:
            marked = False

        job = models.Job.objects.create(user=u, expired=marked)

        for x in range(random.randint(1, 4)):
            job.output.create(path=f'/test/{u.id}/{job.id}/test-{x}.nc')

        jobs.append(job)

        if expired:
            updated_date = now - datetime.timedelta(30)
        else:
            updated_date = now

        s = job.status.create(status=models.ProcessStarted)

        s.updated_date = updated_date

        s.save()

    return jobs

@pytest.fixture
def db_users(db):
    return _create_users()

@pytest.fixture
def db_jobs(db, db_users):
    jobs = _create_jobs(db_users)

    return db_users, jobs

def test_remove_rogue_files(mocker, db_jobs):
    files = models.Output.objects.all().values_list('path', flat=True)

    remove = mocker.patch('compute_wps.tasks.os.remove')

    list_files = mocker.patch('compute_wps.tasks.list_files')

    expected_files = [
        '/data/test1.nc',
        '/data/test2.nc',
        '/data/test3.nc',
        '/data/test4.nc',
    ]

    list_files.return_value = expected_files

    tasks.remove_rogue_files()

    remove.assert_called()

    assert remove.call_count == len(expected_files)

def test_remove_expired_jobs(mocker, db_jobs):
    expired = models.Job.objects.filter(expired=True)

    remove_count = sum(len(x.output.all()) for x in expired)

    remove = mocker.patch('compute_wps.signals.os.remove')

    output = tasks.remove_expired_jobs()

    assert output == expired.count()
    assert remove.call_count == remove_count

def test_notify_users(mocker, db_jobs):
    send_mail = mocker.patch('compute_wps.tasks.mail.send_mail')

    send_mail.side_effect = [1, 1, 1]

    mocker.patch.dict(os.environ, {
        'JOB_EXPIRATION': '30',
        'JOB_EXPIRATION_WARN': '10',
        })

    now = datetime.datetime(2020, 4, 15)

    mocker.patch('compute_wps.tasks.timezone.now', return_value=now)

    expired = models.Job.objects.annotate(
            updated_date=aggregates.Max('status__updated_date')).filter(
                    updated_date__lte=now-datetime.timedelta(20), expired=False)

    groups = tasks.group_by_user(expired)

    test = list(groups.items())[0]

    html = tasks.TEMPLATE.render(**test[1])

    tasks.notify_users(groups)

    send_mail.assert_any_call('LLNL ESGF Compute', html, settings.ADMIN_EMAIL, [test[0],], html_message=html)

    expired = models.Job.objects.annotate(
            updated_date=aggregates.Max('status__updated_date')).filter(
                    updated_date__lte=timezone.now()-datetime.timedelta(20), expired=False)

    assert expired.count() == 0

def test_group_by_user(mocker, db_jobs):
    mocker.patch.dict(os.environ, {
        'JOB_EXPIRATION': '30',
        'JOB_EXPIRATION_WARN': '10',
        })

    expired = models.Job.objects.annotate(
            updated_date=aggregates.Max('status__updated_date')).filter(
                    updated_date__lte=timezone.now()-datetime.timedelta(20), expired=False)

    groups = tasks.group_by_user(expired)

    assert len(groups) == len(set(x.user.id for x in expired))

def test_gather_expired_jobs(mocker, db_jobs):
    mocker.patch.dict(os.environ, {
        'JOB_EXPIRATION': '30',
        'JOB_EXPIRATION_WARN': '10',
        })

    expected = models.Job.objects.annotate(
            updated_date=aggregates.Max('status__updated_date')).filter(
                    updated_date__lte=timezone.now()-datetime.timedelta(20), expired=False)

    expired = tasks.gather_expired_jobs()

    assert expired.count() == expected.count()
    assert all([x == False for x in expired.values_list('expired', flat=True)])

def test_setup_period_tasks(mocker):
    sender = mocker.MagicMock()

    tasks.setup_periodic_tasks(sender)

    sender.add_periodic_task.assert_called()

def test_filename():
    output = tasks.filename('/test/data.nc')

    assert output == 'data.nc'

def test_datetime_format():
    input = datetime.datetime(1990, 12, 12)

    output = tasks.datetime_format(input)

    assert output == 'Wednesday, 12 Dec 90'
