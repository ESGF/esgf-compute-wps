#! /usr/bin/env python

import time
from base64 import b64decode

import mock
import cwt

from django import test
from django.contrib.auth.models import User
from django.db.models import DateTimeField
from compute_wps import models
from compute_wps import metrics

class ModelsFileTestCase(test.TestCase):
    name = "test.nc"
    host = "datanode.domain.com"
    var_name = "test_var"
    url = "http://{h}/thredds/{f}".format(h=host, f=name)

    def setUp(self):
        user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        var = cwt.Variable(self.url, self.var_name, name='tas')
        models.File.track(user, var)
        self.file = models.File.objects.first()

    def test_File_track(self):
        self.assertEqual(self.file.name, self.name)
        self.assertEqual(self.file.host, self.host)
        self.assertEqual(self.file.variable, self.var_name)
        self.assertEqual(self.file.url, self.url)
        self.assertEqual(self.file.requested, 1)

    def test_File_to_json(self):
        file_json = self.file.to_json()
        self.assertEqual(file_json['name'], self.name)
        self.assertEqual(file_json['host'], self.host)
        self.assertEqual(file_json['variable'], self.var_name)
        self.assertEqual(file_json['url'], self.url)

    def test_File_str(self):
        self.assertEqual(str(self.file), self.name)

class ModelsUserFileTestCase(test.TestCase):
    name = "test1.nc"
    host = "datanode.domain.com"
    var_name = "test_var"
    url = "http://{h}/thredds/{f}".format(h=host, f=name)
    
    def setUp(self):
        user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        var = cwt.Variable(self.url, self.var_name, name='tas')
        models.File.track(user, var)
        file = models.File.objects.first()
        self.user_file = models.UserFile.objects.create(user=user, file=file)

    def test_UserFile_to_json(self):
        user_file_json = self.user_file.to_json()
        self.assertEqual(user_file_json['name'], self.name)
        self.assertEqual(user_file_json['host'], self.host)
        self.assertEqual(user_file_json['variable'], self.var_name)
        self.assertEqual(user_file_json['url'], self.url)
        self.assertEqual(user_file_json['requested'], 0)

    def test_UserFile_str(self):
        self.assertEqual(str(self.user_file), self.name)

class ModelsProcessTestCase(test.TestCase):

    def setUp(self):
        self.user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        self.process = models.Process(identifier='test_proc_id', version='1.0.0')
        self.process.save()
        self.process.track(self.user)

    def test_Process_track(self):
        the_process = models.Process.objects.first()
        self.assertEqual(the_process.identifier, 'test_proc_id')

        user_process_1 = models.UserProcess.objects.get(user=self.user)
        self.assertEqual(user_process_1.requested, 1)
        user_process_2 = models.UserProcess.objects.get(process=self.process)
        self.assertEqual(user_process_1, user_process_2)

    def test_Process_to_json(self):
        the_process = models.Process.objects.first()
        the_process_json = the_process.to_json()
        self.assertEqual(the_process_json['identifier'], 'test_proc_id')

    def test_Process_str(self):
        self.assertEqual(str(self.process), self.process.identifier)

class ModelsUserProcessTestCase(test.TestCase):

    identifier = 'test_proc_id'
    def setUp(self):
        # User and Process are ForeignKey of UserProcess
        user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        self.process = models.Process(identifier=self.identifier, version='1.0.0')
        self.user_process = models.UserProcess(user=user, process=self.process)

    def test_UserProcess(self):
        self.assertEqual(self.user_process.requested, 0)

    def test_UserProcess_to_json(self):
        user_process_json = self.user_process.to_json()
        self.assertEqual(user_process_json['requested'], 0)

    def test_UserProcess_str(self):
        self.assertEqual(str(self.user_process), self.identifier)

class ModelsServerTestCase(test.TestCase):
    server_host1 = "test_host1"
    server_host2 = "test_host2"

    def setUp(self):
        self.server1 = models.Server(host=self.server_host1)
        self.server1.save()
        self.server2 = models.Server(host=self.server_host2)
        self.server2.save()

    def test_Server(self):
        '''
        verify the ManyToManyField relationship between Server and Process
        '''
        self.user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        process1 = models.Process(identifier='test_proc_id1', version='1.0.0')
        process1.save()
        process1.track(self.user)

        process2 = models.Process(identifier='test_proc_id2', version='1.0.0')
        process2.save()
        process2.track(self.user)

        self.server1.processes.add(process1)
        self.server1.processes.add(process2)

        self.server2.processes.add(process1)

        processes = self.server1.processes.all()
        self.assertEqual(len(processes), 2)

        servers = process1.server_set.all()
        self.assertEqual(len(servers), 2)

    def test_Server_str(self):
        self.assertEqual(str(self.server1), self.server_host1)
        self.assertEqual(str(self.server2), self.server_host2)
        self.assertEqual(self.server1.status, 1)

class ModelsJobTestCase(test.TestCase):

    job_started_msg = "Job Started"

    def setUp(self):
        self.user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        self.process = models.Process(identifier='test_proc_id1', version='1.0.0')
        self.process.save()
        self.process.track(self.user)

    def test_Job_not_accepted_yet(self):
        self.job = models.Job(user=self.user, process=self.process)
        self.job.save()
        self.assertEqual('Unknown', self.job.accepted_on)
        self.job.delete()

    def test_Job_accepted(self):
        self.job = models.Job(user=self.user, process=self.process)
        self.job.save()
        self.job.accepted()
        self.assertEqual(self.job.status.latest('created_date').status, models.ProcessAccepted)
        self.assertEqual(self.job.status.latest('created_date').created_date.isoformat(), self.job.accepted_on)
        self.job.delete()

    def ABCtest_Job_report_DOES_NOT_WORK(self):
        extra = '{"extra": "extra_val"}'
        self.job = models.Job(user=self.user, process=self.process, extra=extra)
        self.job.save()
        self.job.accepted()
        time.sleep(1)
        self.job.status.create(status='ProcessStarted')
        time.sleep(1)
        self.job.status.create(status='ProcessSucceeded')
        response = self.job.report
        self.assertContains(response, 'wps:ExecuteResponse')
        self.job.delete()

class ModelsOutputTestCase(test.TestCase):

    def test_Output(self):
        user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        process = models.Process(identifier='test_proc_id1', version='1.0.0')
        process.save()
        process.track(user)
        job = models.Job(user=user, process=process)
        job.save()

        output1 = models.Output(job=job)
        output1.save()

        output2 = models.Output(job=job)
        output2.save()

        the_job = models.Job.objects.first()
        outputs = the_job.output.all()
        self.assertEqual(len(outputs), 2)

        # QUESTION:
        # NOT SURE what to check / assert here..
        # Output class has 'path = models.URLField()'

class ModelsStatusTestCase(test.TestCase):

    def setUp(self):
        user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        process = models.Process(identifier='test_proc_id1', version='1.0.0')
        process.save()
        process.track(user)
        self.job = models.Job(user=user, process=process)
        self.job.save()

    def test_set_message_no_percent(self):
        self.status = models.Status(job=self.job)
        self.status.save()
        msg = "job started test message"
        self.status.set_message(msg)
        self.assertEqual(self.status.latest_message, msg)
        self.assertEqual(self.status.latest_percent, None)

    def test_set_message(self):
        self.status = models.Status(job=self.job)
        self.status.save()
        msg = "job started test message"
        percent = 88
        self.status.set_message(msg, percent)
        self.assertEqual(self.status.latest_message, msg)
        self.assertEqual(self.status.latest_percent, percent)

    def test_exception_clean(self):
        exception = "<ows:ExceptionReport xmlns:ows=\"http://www.opengis.net/ows/1.1\" xmlns:compute_wps=\"http://www.opengis.net/compute_wps/1.0.0\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" version=\"1.0.0\">\n  <ows:Exception exceptionCode=\"NoApplicableCode\">\n    <ows:ExceptionText>Job Failed</ows:ExceptionText>\n  </ows:Exception>\n</ows:ExceptionReport>\n"

        clean_exception = "<ows:ExceptionReport>\n  <ows:Exception exceptionCode=\"NoApplicableCode\">\n    <ows:ExceptionText>Job Failed</ows:ExceptionText>\n  </ows:Exception>\n</ows:ExceptionReport>\n"

        self.status = models.Status(job=self.job, exception=exception)
        self.status.save()

        msg = "job failed test message"
        self.status.set_message(msg)
        self.assertEqual(self.status.exception_clean, clean_exception)

