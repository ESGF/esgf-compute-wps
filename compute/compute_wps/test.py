import requests
import coreapi

session = requests.Session()

auth = coreapi.auth.BasicAuthentication("default_api_username", "default_api_password")

transport = coreapi.transports.HTTPTransport(auth=auth, session=session)

client = coreapi.Client(transports=[transport,])

schema = client.get("http://10.101.15.196:8000/internal_api/schema")
