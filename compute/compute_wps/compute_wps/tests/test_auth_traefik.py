import pytest

from compute_wps import models
from compute_wps.auth import traefik
from compute_wps import exceptions

@pytest.mark.django_db
def test_authenticate(mocker):
    meta = {}

    with pytest.raises(exceptions.AuthError):
        traefik.authenticate(meta)

    spy_user = mocker.spy(models.User.objects, "get_or_create")

    meta = {"X-Forwarded-User": "user1@domain1.test"}

    user = traefik.authenticate(meta)

    assert user.username == "user1"

    user = traefik.authenticate(meta)

    assert spy_user.call_count == 2

def test_traefikauthentication(mocker):
    auth = traefik.TraefikAuthentication()

    class Request:
        def META(self):
            return "user1"

    authenticate = mocker.patch.object(traefik, "authenticate")
    authenticate.return_value = None

    user = auth.authenticate(Request())

    assert user is None

    authenticate.return_value = "user1"

    user = auth.authenticate(Request())

    assert user == ("user1", None)
