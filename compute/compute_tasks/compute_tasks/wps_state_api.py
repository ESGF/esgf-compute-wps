import logging
import os
import socket
import time
from urllib.parse import urlparse

import coreapi
from coreapi import document
from coreapi.transports import http
from oauthlib import oauth2
import requests
import requests_oauthlib

import compute_tasks
from compute_tasks import utilities

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "true"

AUTH_ENABLED = utilities.to_bool(os.environ.get("AUTH_ENABLED", "false"))

if AUTH_ENABLED:
    CLIENT_ID = os.environ["CLIENT_ID"]
    CLIENT_SECRET = os.environ["CLIENT_SECRET"]
    TOKEN_URL = os.environ["TOKEN_URL"]

API_SCHEMA_URL = os.environ["API_SCHEMA_URL"]

logger = logging.getLogger("compute_tasks.wps_state_api")


class APIError(compute_tasks.WPSError):
    pass


class HTTPTransport(coreapi.transports.base.BaseTransport):
    schemes = ["http", "https"]

    def __init__(self, session):
        self._session = session

    def transition(
        self,
        link,
        decoders,
        params=None,
        link_ancestors=None,
        force_codec=False,
    ):
        method = http._get_method(link.action)
        encoding = http._get_encoding(link.encoding)
        params = http._get_params(method, encoding, link.fields, params)
        url = http._get_url(link.url, params.path)
        headers = http._get_headers(url, decoders)

        kwargs = {
            "headers": headers,
        }

        if params.query:
            kwargs["params"] = params.query

        if params.data:
            kwargs["data"] = params.data

        logger.info(f"Request {method} {url} {kwargs}")

        response = self._session.request(method, url, **kwargs)

        logger.info(f"Response {response.status_code} {response.text}")

        result = http._decode_result(response, decoders, force_codec)

        if isinstance(result, document.Document) and link_ancestors:
            result = http._handle_inplace_replacement(
                result, link, link_ancestors
            )

        if isinstance(result, document.Error):
            raise coreapi.exceptions.ErrorMessage(result)

        return result


class WPSStateAPI:
    def __init__(self):
        self._client = None
        self._document = None

        self._token = None

    def wait_api(self):
        parts = urlparse(API_SCHEMA_URL)
        host = parts.hostname

        while True:
            try:
                socket.getaddrinfo(host, 0, 0, 0)
            except Exception:
                logger.exception(f"{host} not avaialble")
            else:
                break

            time.sleep(2)

    def init_client(self):
        if AUTH_ENABLED:
            client = oauth2.BackendApplicationClient(client_id=CLIENT_ID)

            session = requests_oauthlib.OAuth2Session(client=client)

            self._token = session.fetch_token(
                TOKEN_URL, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
            )

            def token_updater(token):
                self._token = token

            session = requests_oauthlib.OAuth2Session(
                client=client,
                token=self._token,
                auto_refresh_kwargs={
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                },
                auto_refresh_url=TOKEN_URL,
                token_updater=token_updater,
            )
        else:
            session = requests.Session()

        transport = HTTPTransport(session)

        self._client = coreapi.Client(
            transports=[
                transport,
            ]
        )

        self._document = self._client.get(API_SCHEMA_URL)

    def _action(self, keys, params=None, **kwargs):
        if self._client is None:
            self.init_client()

        retry_count = kwargs.pop("retry_count", 4)
        retry_delay = kwargs.pop("retry_delay", 1)
        filter = kwargs.pop("filter", None)

        r = utilities.retry(
            count=retry_count, delay=retry_delay, filter=filter
        )(self._client.action)

        try:
            return r(self._document, keys, params=params, **kwargs)
        except utilities.RetryExceptionWrapper as wrapper:
            raise wrapper.e
        except Exception as e:
            raise APIError(str(e))

    def started(self, job):
        return self._job_status(job, "ProcessStarted")

    def succeeded(self, job, output):
        id = self._job_status(job, "ProcessSucceeded")

        self.message(id, output)

        return id

    def failed(self, job, exception):
        id = self._job_status(job, "ProcessFailed")

        self.message(id, exception)

        return id

    def message(self, status, message, percent=None):
        params = {
            "status": status,
            "message": message,
            "percent": percent or 0.0,
        }

        self._action(["message", "create"], params)

    def _job_status(self, job, status):
        params = {
            "job": job,
            "status": status,
        }

        response = self._action(["status", "create"], params)

        return response["id"]

    def process_create(
        self, identifier, abstract=None, metadata=None, version=None
    ):
        params = {
            "identifier": identifier,
            "abstract": abstract or "",
            "metadata": metadata or {},
            "version": version or "1.0.0",
        }

        def filter_exceptions(e):
            if isinstance(
                e, coreapi.exceptions.ErrorMessage
            ) and "unique set" in str(e):
                return True

            return False

        try:
            self._action(
                ["process", "create"], params, filter=filter_exceptions
            )
        except coreapi.exceptions.ErrorMessage:
            pass

    def output_create(self, job, local, remote, size):
        params = {
            "job": job,
            "local": local,
            "remote": remote,
            "size": round(size, 4),
        }

        response = self._action(["output", "create"], params)

        return response["id"]
