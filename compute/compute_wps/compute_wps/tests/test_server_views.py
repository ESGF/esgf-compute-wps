import pytest
from django import test
from compute_wps import models
from compute_wps.views import service

class ServerViewsTestCase(test.TestCase):
    def test_home(self):
        response = self.client.get('/wps/')

        self.assertEqual(response.status_code, 200)

@pytest.mark.django_db(transaction=True)
def test_handle_execute_traefik_auth(mocker, settings, django_user_model):
    settings.AUTH_KEYCLOAK = False
    settings.AUTH_TRAEFIK = True

    models.Process.objects.create(identifier='CDAT.subset', version='1.0')

    meta = {
        'X-Forwarded-User': 'test@site.com'
    }
    identifier = 'CDAT.subset'
    data_inputs = {
        'variable': [],
        'domain': [],
        'operation': [],
    }

    mocker.patch('compute_wps.views.service.send_request_provisioner')

    result = service.handle_execute(meta, identifier, data_inputs)

    assert result

    service.handle_execute(meta, identifier, data_inputs)
