import os
import logging

from django.conf import settings
from django.core.mail import EmailMessage
from django.db.models.signals import post_delete
from django.db.models.signals import post_save
from django.dispatch import receiver

from compute_wps import models

logger = logging.getLogger('compute_wps.signals')

JOB_SUCCESS_MSG = """
Hello {first_name},

<pre>
Job {job_id} has finished successfully.
</pre>

<pre>
<a href="{job_url}">View job history</a>
</pre>

<pre>
Thank you,
ESGF Compute Team
</pre>
"""

JOB_FAILED_MSG = """
Hello {first_name},

<pre>
Job {job_id} has failed.
</pre>

<pre>
{exception}
</pre>

Report errors on <a href="https://github.com/ESGF/esgf-compute-wps/issues/new?template=Bug_report.md">GitHub</a>.

<pre>
Thank you,
ESGF Compute Team
</pre>
"""


def send_failed_email(job, exception):
    logger.info('Sending failed email to user %r', job.user.id)

    kwargs = {
        'first_name': job.user.first_name,
        'job_id': job.id,
        'exception': exception,
    }

    msg = JOB_FAILED_MSG.format(**kwargs)

    email = EmailMessage('Job Failed', msg, to=[job.user.email, ])

    email.content_subtype = 'html'

    email.send(fail_silently=True)


def send_success_email(job):
    logger.info('Sending success email to user %r', job.user.pk)

    kwargs = {
        'first_name': job.user.first_name,
        'job_id': job.id,
        'job_url': settings.JOBS_URL,
    }

    msg = JOB_SUCCESS_MSG.format(**kwargs)

    email = EmailMessage('Job Success', msg, to=[job.user.email, ])

    email.content_subtype = 'html'

    email.send(fail_silently=True)


@receiver(post_save, sender=models.Status)
def save_job_handler(sender, instance, **kwargs):
    logger.info('Handling job status %r', instance.status)

    if instance.status == 'ProcessSucceeded':
        send_success_email(instance.job)
    elif instance.status == 'ProcessFailed':
        send_failed_email(instance.job, instance.exception)
