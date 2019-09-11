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

def get_association_vars(i):
    a = {}
    a['handle'] = "handle{}".format(i)
    a['secret'] = bytes("secret{}".format(i), 'utf-8')
    return a

def create_association(index, server_url, lifetime):
    user = models.User.objects.create_user("user{}".format(index),
                                           "email{}@test.com".format(index),
                                           first_name="first_name{}".format(index),
                                           last_name="last_name{}".format(index))

    assoc_vars = get_association_vars(index)
    issued = int(time.time())
    assoc = models.OpenIDAssociation(user=user, server_url=server_url,
                                     handle=assoc_vars['handle'], lifetime=lifetime,
                                     secret=assoc_vars['secret'], issued=issued,
                                     assoc_type='HMAC-SHA1')
    return user, assoc_vars, issued, assoc

class ModelsDjangoOpenIDStoreTestCase(test.TestCase):

    def test_storeAssociation(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 3600
        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        a = models.OpenIDAssociation.objects.first()
        self.assertEqual(a.server_url, server_url)
        self.assertEqual(a.handle, assoc_vars['handle'])
        self.assertEqual(a.issued, issued)
        self.assertEqual(a.lifetime, 3600)
        self.assertEqual(b64decode(a.secret), assoc_vars['secret'])

    def test_storeAssociation_already_exists(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 3600
        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        lifetime2 = 1200
        secret2 = bytes("new_secret", 'utf-8')
        assoc.lifetime = lifetime2
        assoc.secret = secret2
        openid_store.storeAssociation(server_url, assoc)
        a = models.OpenIDAssociation.objects.first()
        self.assertEqual(a.server_url, server_url)
        self.assertEqual(a.handle, assoc_vars['handle'])
        self.assertEqual(a.issued, issued)
        self.assertEqual(a.lifetime, lifetime2)
        self.assertEqual(b64decode(a.secret), secret2)

    def test_getAssociation(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 3600
        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        active_assoc = openid_store.getAssociation(server_url, assoc_vars['handle'])
        self.assertEqual(active_assoc.handle, assoc_vars['handle'])

    def test_getAssociation_not_specify_handle(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 3600

        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        active_assoc = openid_store.getAssociation(server_url, None)
        self.assertEqual(active_assoc.handle, assoc_vars['handle'])
        
    def test_getAssociation_expired(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 1

        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        time.sleep(lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        active_assoc = openid_store.getAssociation(server_url, None)
        self.assertEqual(active_assoc, None)

    def test_getAssociation_multiple_diff_servers(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 60
        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        server_url2 = "https://identity_server_url2"
        index2 = 2
        lifetime2 = 45
        user2, assoc_vars2, issued2, assoc2 = create_association(index2,
                                                                 server_url2,
                                                                 lifetime2)
        openid_store.storeAssociation(server_url2, assoc2)

        active_assoc = openid_store.getAssociation(server_url, None)
        self.assertEqual(active_assoc.handle, assoc_vars['handle'])        

    def test_getAssociation_multiple_same_servers(self):
        '''
        Have a server that has more than one associations.
        call getAssociation() without specifying handle, and verify
        that getAssociation() returns the one recently issued.
        
        '''
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 60
        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        index2 = 2
        lifetime2 = 60
        user2, assoc_vars2, issued2, assoc2 = create_association(index2,
                                                                 server_url,
                                                                 lifetime2)
        openid_store.storeAssociation(server_url, assoc2)

        active_assoc = openid_store.getAssociation(server_url, None)
        self.assertEqual(active_assoc.handle, assoc_vars2['handle'])        

    def test_removeAssociation(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 3600
        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)
        ret = openid_store.removeAssociation(server_url, assoc.handle)
        self.assertTrue(ret)

    def test_removeAssociation_not_exists(self):
        server_url = "https://identity_server_url"
        openid_store = models.DjangoOpenIDStore()
        ret = openid_store.removeAssociation(server_url, "some_handle")
        self.assertFalse(ret)

    def test_cleanupAssociations(self):
        server_url = "https://identity_server_url"
        index = 1
        lifetime = 1
        user, assoc_vars, issued, assoc = create_association(index,
                                                             server_url,
                                                             lifetime)
        openid_store = models.DjangoOpenIDStore()
        openid_store.storeAssociation(server_url, assoc)

        server_url2 = "https://identity_server2_url"
        index2 = 2
        lifetime2 = 1
        user2, assoc_vars2, issued2, assoc2 = create_association(index2,
                                                                 server_url2,
                                                                 lifetime2)
        openid_store.storeAssociation(server_url2, assoc2)
        time.sleep(2)
        count = openid_store.cleanupAssociations()
        self.assertEqual(count, 2)

    def test_useNonce_did_not_exist(self):
        server_url = "https://never_used_server_url"
        timestamp = time.time()
        salt = "abc"
        openid_store = models.DjangoOpenIDStore()
        ret = openid_store.useNonce(server_url, timestamp, salt)
        self.assertTrue(ret)
        ret = openid_store.useNonce(server_url, timestamp, salt)
        self.assertFalse(ret)

    def test_useNonce_exceed_skew(self):
        '''
        verify that if the timestamp window exceeds nonce.skew 
        useNonce returns False
        '''
        server_url = "https://never_used_server_url"
        timestamp = time.time() - models.nonce.SKEW - 1
        salt = "abc"
        openid_store = models.DjangoOpenIDStore()
        ret = openid_store.useNonce(server_url, timestamp, salt)        
        self.assertFalse(ret)

    def test_cleanupNonces_not_expired(self):
        '''
        verify that if nonce has not expired yet, cleanupNonce() does not 
        delete it.
        '''
        server_url = "https://never_used_server_url"
        timestamp = time.time()
        salt = "abc"
        openid_store = models.DjangoOpenIDStore()
        ret = openid_store.useNonce(server_url, timestamp, salt)
        count = openid_store.cleanupNonces()
        self.assertEqual(count, 0)

    def test_cleanupNonce_expired(self):
        '''
        verify that if nonce has expired, cleanupNonce() deletes it.
        '''
        server_url = "https://never_used_server_url"
        wait_seconds = 2
        timestamp = int(time.time()) - models.nonce.SKEW + wait_seconds
        salt = "defg"
        openid_store = models.DjangoOpenIDStore()
        ret = openid_store.useNonce(server_url, timestamp, salt)
        self.assertTrue(ret)
        time.sleep(wait_seconds + 1)
        count = openid_store.cleanupNonces()
        self.assertEqual(count, 1)

class ModelsOpenIDNonce(test.TestCase):
    server_url = "https://identity_server_url"
    def setUp(self):
        self.user = models.User.objects.create_user("user",
                                                    "email@test.com",
                                                    first_name="first_name",
                                                    last_name="last_name")
        self.openid_nonce = models.OpenIDNonce(user=self.user,
                                               server_url=self.server_url)
    def test_OpenIDNonce(self):
        self.assertEqual(self.openid_nonce.user.username, self.user.username)
        self.assertEqual(self.openid_nonce.server_url, self.server_url)

    def test_OpenIDNonce_str(self):
        self.assertEqual(str(self.openid_nonce), self.server_url)

class ModelsOpenIDAssociationTestCase(test.TestCase):
    lifetime = 3600
    server_url = "https://identity_server_url"

    def setUp(self):
        self.user, self.vars, self.issued, self.assoc = create_association(1, 
                                                                           self.server_url, 
                                                                           self.lifetime)

    def test_OpenIDAssocation(self):
        self.assertEqual(self.assoc.user.username, self.user.username)
        self.assertEqual(self.assoc.lifetime, self.lifetime)
        self.assertEqual(self.assoc.handle, self.vars['handle'])
        self.assertEqual(self.assoc.secret, self.vars['secret'])

    def test_OpenIDAssocation_str(self):
        self.assertEqual(str(self.assoc), "{s} {h}".format(s=self.server_url, h=self.vars['handle']))

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

class ModelsAuthTestCase(test.TestCase):
    openid_url = 'http://test.com/openid'
    type = 'test_auth_type'

    def setUp(self):
        user = models.User.objects.create_user('test_user1', 'test_email@test.com', 'test_password1')
        self.auth = models.Auth.objects.create(openid_url=self.openid_url, user=user)

    def test_Auth_generate_api_key(self):
        self.auth.generate_api_key()
        self.assertTrue(len(self.auth.api_key) > 0)

    def test_Auth_update(self):
        certs = ['cert1', 'cert2']
        api_key = 'updated_api_key'

        self.auth.update(self.type, certs, api_key, some_extra_attr='some_extra_val')
        self.assertEqual(self.auth.type, self.type)
        self.assertEqual(self.auth.cert, "".join(certs))
        self.assertEqual(self.auth.api_key, api_key)
        self.assertEqual(self.auth.extra, '{"some_extra_attr": "some_extra_val"}')
        
    def test_Auth_get(self):        
        self.auth.update(None, None, None, extra_attr1='extra_val1', extra_attr2='extra_val2')
        vals = self.auth.get('extra_attr1', 'extra_attr2')
        self.assertEqual(vals[0], 'extra_val1')
        self.assertEqual(vals[1], 'extra_val2')

    def test_Auth_str(self):
        self.auth.update(self.type, None, None)
        self.assertEqual(str(self.auth), self.openid_url + ' ' + self.type)

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

    def ABCtest_Job_report(self):
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

