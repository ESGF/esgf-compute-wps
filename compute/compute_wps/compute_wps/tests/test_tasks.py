import datetime
import re

from unittest import mock
from django import test
from django.utils import timezone

from compute_wps import models
from compute_wps import tasks


class TasksTestCase(test.TestCase):
    def setUp(self):
        self.user1 = models.User.objects.create(username='test1', email='test1@site.org',
                                                first_name="John", last_name="Smith")

        self.user2 = models.User.objects.create(username='test2', email='test2@site.org',
                                                first_name="Jennifer", last_name="Lawrence")

        now = timezone.now()

        for x in range(64):
            expired = True if x > 32 and x < 55 else False

            job = models.Job.objects.create(user=self.user1, expired=expired)

            job.output.create(path='/test/user-{!s}/job-{!s}/test.nc'.format(self.user1.id, job.id))

            status = job.status.create(status=models.ProcessStarted)

            status.updated_date = now - datetime.timedelta(days=x)

            status.save()

            if x < 32:
                expired = True if x > 30 else False

                job = models.Job.objects.create(user=self.user2, expired=expired)

                job.output.create(path='/test/user-{!s}/job-{!s}/test.nc'.format(self.user2.id, job.id))

                status = job.status.create(status=models.ProcessStarted)

                status.updated_date = now - datetime.timedelta(days=x)

                status.save()

    @mock.patch('compute_wps.tasks.mail')
    def test_warn_expired_jobs(self, mock_mail):
        print(models.Job.objects.all().update(expired=False))

        mock_mail.send_mail.return_value = 1

        tasks.check_expired_jobs()

        mock_mail.send_mail.assert_called()

        self.assertEqual(mock_mail.send_mail.call_count, 2)

        jobs = models.Job.objects.all()

        self.assertEqual(len(jobs), 96)

        expired = models.Job.objects.filter(expired=True)

        self.assertEqual(len(expired), 56)

        user1_msg = mock_mail.send_mail.call_args_list[0][1]['html_message']

        self.assertIn(self.user1.get_full_name(), user1_msg)

        self.assertEqual(len(re.findall('<li><a href=".*">.*</a></li>', user1_msg)), 44)

        user2_msg = mock_mail.send_mail.call_args_list[1][1]['html_message']

        self.assertIn(self.user2.get_full_name(), user2_msg)

        self.assertEqual(len(re.findall('<li><a href=".*">.*</a></li>', user2_msg)), 12)

    def test_remove_expired_jobs_set_to_expire(self):
        tasks.remove_expired_jobs()

        jobs = models.Job.objects.all()

        self.assertEqual(len(jobs), 73)

    def test_remove_expired_jobs(self):
        # Reset all jobs to unexpired
        models.Job.objects.all().update(expired=False)

        tasks.remove_expired_jobs()

        jobs = models.Job.objects.all()

        self.assertEqual(len(jobs), 96)
