# Authentication

The WPS service supports two OAuth2 flows; Client Credentials and Authorization Code with PKCE flows. It also supports being configured with no authentication.

## Client Credentials flow

The LLNL ESGF WPS supports authentication via Client Credentials. To aid in a user retreiving their credentials we have supplied the ability to use KeyCloaks dynamica client registration. 

The Client Credentials with Dynamic Client Registristration workflow using the example below.

- User is given a url to open in a browser
- Url will redirect the user to login via KeyCloak
- If user is authenticated the server will dyanmically register a new client for the user
- Their Client ID and Client sercret are displayed once
- The End-user API will use these id and secret to retrieve access tokens to pass to the WPS which uses token introspection to validate the token. Authorization can be provided by KeyCloak.

```python
auth = cwt.LLNLKeyCloakAuthenticator(
    base_url='https://wps.io',
    keycloak_url='https://wps.io/auth',
    realm='compute',
)
client = cwt.LLNLClient('https://wps.io/wps', auth=auth)
```

## Authorization Code flow

***Note*** In order for this method to work the client must be run in an environment where a local web service can be launched an accessed from the local browser.

The LLNL ESGF WPS also suppots authentication via Authorization Code. To further secure this flow [PKCE verification](https://www.appsdeveloperblog.com/pkce-verification-in-authorization-code-grant/) is used.

Authorization Code with PKCE workflow.

- User is given a url to open in a browser
- Url will redirect the user to login via KeyCloak
- The client launches a local webserver listening on port `8888`
- If the user is authenticated the server will redirect the users browser to `http://127.0.0.1:8888`
- The client then verifies the response using the code challenge and can then request an access token which is passed to the WPS which uses token introspection to validate the token. Authorization can be provided by KeyCloak.

```python
auth = cwt.LLNLKeyCloakAuthenticator(
    base_url='https://wps.io',
    keycloak_url='https://wps.io/auth',
    realm='compute',
    client_id="compute_public",
    pkce=True,
)
client = cwt.LLNLClient('https://wps.io/wps', auth=auth)
```

# Disabling authentication

If the WPS service is launched with the environment variable `AUTH_ENABLED` set to false then no authentication is required.