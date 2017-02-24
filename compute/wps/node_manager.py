#! /usr/bin/env python

import logging
from contextlib import closing

import zmq

from wps.conf import settings

logger = logging.getLogger(__name__)

class NodeManager(object):

    def __get_socket(self, socket_type, host, port):
        context = zmq.Context.instance()

        socket = context.socket(socket_type)

        socket.connect('tcp://{0}:{1}'.format(host, port))

        logger.debug('Opened socket to CDAS instance %s:%s', host, port)

        return socket

    def monitor_responses(self):
        with closing(self.__get_socket(zmq.PULL,
            settings.CDAS_HOST,
            settings.CDAS_RESPONSE_PORT)) as response:

            while True:
                result = response.recv()

                logger.info(result)

    def execute(self, identifier, data_inputs):
        with closing(self.__get_socket(zmq.PUSH,
            settings.CDAS_HOST,
            settings.CDAS_REQUEST_PORT)) as request:

            request.send(str('1!execute!{0}!{1}'.format(identifier, data_inputs)))

        return True

    def describe_process(self, identifier):
        with closing(self.__get_socket(zmq.PUSH,
            settings.CDAS_HOST,
            settings.CDAS_REQUEST_PORT)) as request:

            request.send(str('1!describeProcess!{0}'.format(identifier)))

        return True
    
    def get_capabilities(self):
        with closing(self.__get_socket(zmq.PUSH,
            settings.CDAS_HOST,
            settings.CDAS_REQUEST_PORT)) as request:

            request.send(str('1!getCapabilities!WPS'))

        return True
