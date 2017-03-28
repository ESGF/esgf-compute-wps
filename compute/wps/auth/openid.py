#! /usr/bin/env python

import logging

import requests
from lxml import etree

logger = logging.getLogger(__name__)

class OpenIDError(Exception):
    pass

class Service(object):

    def __init__(self, priority, types, uri, local_id):
        self.types = types
        self.priority = priority
        self.uri = uri
        self.local_id = local_id

    @classmethod
    def parse_element(cls, element):
        priority = element.attrib.get('priority')

        types =[]

        for t in element.findall('.//{xri://$xrd*($v*2.0)}Type'):
            types.append(t.text)

        uri = element.find('.//{xri://$xrd*($v*2.0)}URI')

        local_id = element.find('.//{xri://$xrd*($v*2.0)}LocalID')

        try:
            return cls(priority, types, uri.text, local_id.text)
        except:
            raise OpenIDError('Failed to parse service')

    def __str__(self):
        return 'Priority: {0} Types: {1} URI: {2} LocalID: {3}'.format(
                self.priority,
                ','.join(self.types),
                self.uri,
                self.local_id)

class OpenID(object):

    def __init__(self):
        self.services = {}

    @classmethod
    def parse(cls, url):
        obj = cls()

        obj._retrieve_and_parse(url)

        return obj

    def _retrieve_and_parse(self, url):
        try:
            response = requests.get(url)
        except requests.ConnectionError:
            raise OpenIDError('Error contacting server')

        if response.status_code not in (200,):
            raise OpenIDError('Server returned status code {}'.format(response.status_code))

        try:
            tree = etree.fromstring(str(response.text))
        except etree.XMLSyntaxeError:
            raise OpenIDError('Failed to load XRDI document')

        services = tree.findall('.//{xri://$xrd*($v*2.0)}Service')

        for s in services:
            service = Service.parse_element(s)

            for t in service.types:
                self.services[t] = service

    def find(self, identifier):
        try:
            return self.services[identifier]
        except KeyError:
            raise OpenIDError('Service is not known')

    def __str__(self):
        return ','.join(self.services.keys())
