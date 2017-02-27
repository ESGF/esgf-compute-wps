#! /usr/bin/env python

import logging
from contextlib import closing

import zmq

from wps import models
from wps.conf import settings

logger = logging.getLogger(__name__)

class NoAvailableServers(Exception):
    pass

class NodeManager(object):

    def __get_socket(self, socket_type, host, port):
        context = zmq.Context.instance()

        socket = context.socket(socket_type)

        socket.connect('tcp://{0}:{1}'.format(host, port))

        logger.debug('Opened socket to CDAS instance %s:%s', host, port)

        return socket

    def get_server(self):
        servers = models.Server.objects.all()

        if len(servers) == 0:
            raise NoAvailableServers()

        return servers[0]

    def monitor_responses(self):
        # TODO should be polling all instance servers for results
        server = self.get_server()

        with closing(self.__get_socket(zmq.PULL, server.address, server.response)) as response:
            while True:
                result = response.recv()
                
                job_id, _, data = result.split('!')

                logger.info('JOB FINISHED %s', job_id)

                job = models.Job.objects.get(pk=job_id)

                job.status = 'completed'

                job.result = data

                job.save()

    def execute(self, identifier, data_inputs):
        server = self.get_server()

        job = models.Job(server=server, status='running')

        job.save()

        with closing(self.__get_socket(zmq.PUSH, server.address, server.request)) as request:
            request.send(str('{2}!execute!{0}!{1}'.format(identifier, data_inputs, job.id)))

        return True

    def describe_process(self, identifier):
        server = self.get_server()

        # TODO refer to get_capabilities, need to map operation to instance
        # if we support aggregation
        with closing(self.__get_socket(zmq.PUSH, server.address, server.request)) as request:
            request.send(str('1!describeProcess!{0}'.format(identifier)))

        return True
    
    def get_capabilities(self):
        server = self.get_server()

        # TODO aggregate all instances to support mixed operation support
        with closing(self.__get_socket(zmq.PUSH, server.address, server.request)) as request:
            request.send(str('1!getCapabilities!WPS'))

        return True
