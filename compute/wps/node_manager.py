#! /usr/bin/env python

from contextlib import closing
import datetime
from lxml import etree
import logging

from esgf.wps_lib import metadata
from esgf.wps_lib import operations
import zmq

from wps import models
from wps.conf import settings

logger = logging.getLogger(__name__)

STATUS_MAP = {
        'accepted': metadata.ProcessAccepted,
        'started': metadata.ProcessStarted,
        'paused': metadata.ProcessPaused,
        'succeeded': metadata.ProcessSucceeded,
        'failed': metadata.ProcessFailed,
        }

class JobDoesNotExist(Exception):
    pass

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

        candidate = None

        for s in servers:
            free = s.queue_size - s.queue

            if free > 0:
                candidate = s

                break

        if candidate is None:
            raise NoAvailableServers()

        return candidate

    def monitor_responses(self, server):
        logger.info('Monitoring for response at %s:%s', server.address, server.response)

        with closing(self.__get_socket(zmq.PULL, server.address, server.response)) as response:
            while True:
                result = response.recv()
                
                job_id, _, data = result.split('!')

                logger.info('JOB FINISHED %s', job_id)

                job = models.Job.objects.get(pk=job_id)

                job.status = 'succeeded'

                job.result = data

                job.server.queue -= 1
                
                job.server.save()

                job.save()

    def get_status(self, job_id):
        try:
            job = models.Job.objects.get(pk=job_id)
        except models.Job.DoesNotExist:
            raise JobDoesNotExist()

        if job.status in ('succeeded'):
            # Build final output with result which should just be data
            response = operations.ExecuteResponse.from_xml(job.result)

            response.service_instance = 'http://aims2.llnl.gov/wps'
            response.creation_time = '{0: %X} {0: %x}'.format(datetime.datetime.now())
            response.status_location = 'http://0.0.0.0:8000/wps/status/{0}'.format(job_id)
            response.status = metadata.ProcessSucceeded()
            response.process = metadata.Process()
            response.process.identifier = 'dummy'
            response.process.title = 'dummy'
            
            result = response.xml()
        else:
            process = metadata.Process() 
            process.identifier = 'dummy'
            process.title = 'dummy'

            response = operations.ExecuteResponse()
            response.service = 'WPS'
            response.version = '1.0.0'
            response.lang = 'en-CA'
            response.service_instance = 'http://aims2.llnl.gov/wps'
            response.creation_time = '{0: %X} {0: %x}'.format(datetime.datetime.now())
            response.status_location = 'http://0.0.0.0:8000/wps/status/{0}'.format(job_id)
            response.status = STATUS_MAP[job.status]()
            response.process = process

            result = response.xml()

        return result

    def execute(self, identifier, data_inputs):
        server = self.get_server()

        server.queue += 1

        server.save()

        logger.info('Executing on server {0}'.format(server.name))

        # Put job creation in the view so we can set initial staus to accepted
        job = models.Job(server=server, status='started')

        job.save()

        with closing(self.__get_socket(zmq.PUSH, server.address, server.request)) as request:
            request.send(str('{2}!execute!{0}!{1}'.format(identifier, data_inputs, job.id)))

        # This should be loaded from database
        process = metadata.Process() 
        process.identifier = 'dummy'
        process.title = 'dummy'

        # Alot of values are static so we'll store a copy a load and 
        # make deep copies as needed
        response = operations.ExecuteResponse()
        response.service = 'WPS'
        response.version = '1.0.0'
        response.lang = 'en-CA'
        response.service_instance = 'http://aims2.llnl.gov/wps'
        response.creation_time = '{0: %X} {0: %x}'.format(datetime.datetime.now())
        response.status_location = 'http://0.0.0.0:8000/wps/status/{0}'.format(job.id)
        response.status = metadata.ProcessStarted()
        response.process = process

        result = response.xml()

        return result

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
