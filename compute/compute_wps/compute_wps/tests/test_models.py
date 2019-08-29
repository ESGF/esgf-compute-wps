#! /usr/bin/env python

import mock
import cwt

from django import test
from django.contrib.auth.models import User

from compute_wps import models

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
        self.assertEqual(str(self.the_file), self.name)

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

class CacheModelTestCase(test.TestCase):
    pass
