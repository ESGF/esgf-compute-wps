import os

import pytest
import requests

from compute_tasks import managers
from compute_tasks import AccessError
from compute_tasks import WPSError


def test_check_access_no_success_status(mocker):
    mocker.patch.object(managers.file_manager.requests, 'get')

    managers.file_manager.requests.get.return_value.status_code = 401

    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    output = fm.check_access('file:///test.nc')

    assert not output

    managers.file_manager.requests.get.assert_called_with('file:///test.nc.dds', timeout=(10, 30), cert=None, verify=False)


def test_check_access_read_timeout(mocker):
    mocker.patch.object(managers.file_manager.requests, 'get')

    managers.file_manager.requests.get.side_effect = requests.ReadTimeout

    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    with pytest.raises(AccessError):
        fm.check_access('file:///test.nc')

    managers.file_manager.requests.get.assert_called_with('file:///test.nc.dds', timeout=(10, 30), cert=None, verify=False)


def test_check_access_connect_timeout(mocker):
    mocker.patch.object(managers.file_manager.requests, 'get')

    managers.file_manager.requests.get.side_effect = requests.ConnectTimeout

    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    with pytest.raises(AccessError):
        fm.check_access('file:///test.nc')

    managers.file_manager.requests.get.assert_called_with('file:///test.nc.dds', timeout=(10, 30), cert=None, verify=False)


def test_check_access_connection_error(mocker):
    mocker.patch.object(managers.file_manager.requests, 'get')

    managers.file_manager.requests.get.side_effect = requests.ConnectionError

    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    with pytest.raises(AccessError):
        fm.check_access('file:///test.nc')

    managers.file_manager.requests.get.assert_called_with('file:///test.nc.dds', timeout=(10, 30), cert=None, verify=False)


def test_check_access(mocker):
    mocker.patch.object(managers.file_manager, 'requests')

    managers.file_manager.requests.get.return_value.status_code = 200

    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    output = fm.check_access('file:///test.nc')

    assert output

    managers.file_manager.requests.get.assert_called_with('file:///test.nc.dds', timeout=(10, 30), cert=None, verify=False)


def test_get_variable_var_missing(mocker):
    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    mocker.patch.object(fm, 'open_file')

    fm.open_file.return_value.__getitem__.return_value = None

    with pytest.raises(WPSError):
        fm.get_variable('/tmp/test.nc', 'tas')

    fm.open_file.assert_called_with('/tmp/test.nc')

    fm.open_file.return_value.__getitem__.assert_called_with('tas')


def test_get_variable(mocker):
    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    mocker.patch.object(fm, 'open_file')

    var = fm.get_variable('/tmp/test.nc', 'tas')

    fm.open_file.assert_called_with('/tmp/test.nc')

    fm.open_file.return_value.__getitem__.assert_called_with('tas')

    assert var == fm.open_file.return_value.__getitem__.return_value


def test_load_certificate_already_exists(mocker):
    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    fm.cert_path = '/tmp/test/test.pem'

    output = fm.load_certificate()

    assert output == '/tmp/test/test.pem'


def test_load_certificate(mocker):
    context = mocker.MagicMock()

    mocker.patch.object(managers.file_manager.os, 'chdir')

    mocker.patch.object(managers.file_manager.FileManager, 'write_user_certificate')

    temp_dir = mocker.MagicMock()
    temp_dir.name = '/tmp/test'

    managers.file_manager.FileManager.write_user_certificate.return_value = (temp_dir, '/tmp/test/test.pem', '/tmp/test/.dodsrc')

    fm = managers.file_manager.FileManager(context)

    output = fm.load_certificate()

    assert output == '/tmp/test/test.pem'

    context.user_cert.assert_called()

    managers.file_manager.os.chdir.assert_called_with('/tmp/test')


def test_write_user_certificate(mocker):
    temp_dir, cert_path, dodsrc_path = managers.file_manager.FileManager.write_user_certificate('cert_data')

    assert os.path.exists(temp_dir.name)
    assert os.path.exists(cert_path)
    assert os.path.exists(dodsrc_path)

    with open(cert_path) as temp:
        data = temp.read()

    assert data == 'cert_data'

    with open(dodsrc_path) as temp:
        data = temp.read()

    assert 'HTTP.COOKIEJAR' in data
    assert 'HTTP.SSL.CERTIFICATE' in data
    assert 'HTTP.SSL.KEY' in data
    assert 'HTTP.SSL.VERIFY=0' in data
    assert '/.dods_cookie' in data
    assert cert_path in data


def test_open_file_error_open(mocker, esgf_data):
    context = mocker.MagicMock()

    local_path = esgf_data.to_local_path('tas')

    fm = managers.file_manager.FileManager(context)

    mocker.patch.object(managers.file_manager, 'cdms2')

    managers.file_manager.cdms2.open.side_effect = Exception

    mocker.patch.object(fm, 'check_access')

    fm.check_access.return_value = True

    with pytest.raises(AccessError):
        fm.open_file(local_path)

    assert local_path not in fm.auth
    assert local_path not in fm.handles

    fm.check_access.assert_called_with(local_path)


def test_open_file_check_access_fail(mocker, esgf_data):
    context = mocker.MagicMock()

    local_path = esgf_data.to_local_path('tas')

    fm = managers.file_manager.FileManager(context)

    mocker.patch.object(fm, 'load_certificate')

    fm.load_certificate.return_value = '/tmp/cert.pem'

    mocker.patch.object(fm, 'check_access')

    fm.check_access.side_effect = [False, False]

    with pytest.raises(WPSError):
        fm.open_file(local_path)

    fm.check_access.assert_any_call(local_path)
    fm.check_access.assert_any_call(local_path, '/tmp/cert.pem')


def test_open_file_check_access_no_cert_fail(mocker, esgf_data):
    context = mocker.MagicMock()

    local_path = esgf_data.to_local_path('tas')

    fm = managers.file_manager.FileManager(context)

    mocker.patch.object(fm, 'load_certificate')

    fm.load_certificate.return_value = '/tmp/cert.pem'

    mocker.patch.object(fm, 'check_access')

    fm.check_access.side_effect = [False, True]

    data = fm.open_file(local_path)

    assert local_path == data.id
    assert local_path in fm.auth
    assert local_path in fm.handles

    fm.check_access.assert_any_call(local_path)
    fm.check_access.assert_any_call(local_path, '/tmp/cert.pem')


def test_open_file(mocker, esgf_data):
    context = mocker.MagicMock()

    local_path = esgf_data.to_local_path('tas')

    fm = managers.file_manager.FileManager(context)

    mocker.patch.object(fm, 'check_access')

    fm.check_access.return_value = True

    data = fm.open_file(local_path)

    assert local_path == data.id
    assert local_path not in fm.auth
    assert local_path in fm.handles

    fm.check_access.assert_called_with(local_path)


def test_requires_cert_required(mocker):
    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    fm.auth.append('file:///test1.nc')

    assert fm.requires_cert('file:///test1.nc')


def test_requires_cert(mocker):
    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    assert not fm.requires_cert('file:///test1.nc')


def test_file_manager(mocker):
    mocker.patch.object(managers.file_manager, 'os')

    context = mocker.MagicMock()

    fm = managers.file_manager.FileManager(context)

    del fm

    managers.file_manager.os.chdir.assert_not_called()
