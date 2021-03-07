import base64
import datetime
import hashlib
import json
import logging
import os
import threading
import time
from collections import OrderedDict
from uuid import uuid4
from functools import partial

import yaml
import zmq
from prometheus_client import start_http_server

from compute_provisioner import allocator
from compute_provisioner import constants
from compute_provisioner import metrics
from compute_provisioner import kube_cluster

logger = logging.getLogger('provisioner')

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LIFETIME = int(os.environ.get('LIFETIME', '120'))
NAMESPACE = os.environ.get('NAMESPACE', 'default')
SA_NAME = os.environ.get('SERVICE_ACCOUNT_NAME', 'compute')
IMAGE_PULL_SECRET = os.environ.get('IMAGE_PULL_SECRET', 'docker-registry')
FRONTEND_PORT = 7777
BACKEND_PORT = 7778

class ResourceAllocationError(Exception):
    pass

class RemoveWorkerError(Exception):
    pass

def json_encoder(x):
    def default(y):
        return {'data': y.decode(constants.ENCODING)}

    return json.dumps(x, default=default)


def json_decoder(x):
    def object_hook(y):
        if 'data' in y:
            return y['data'].encode(constants.ENCODING)

        return y

    return json.loads(x, object_hook=object_hook)

class State(object):
    def on_event(self, **event):
        pass

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.__class__.__name__

class WaitingResourceRequestState(State):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

        self.queue = kwargs['queue']

        self.resources = kwargs['resources']

        self.lifetime = kwargs['lifetime']

        self.backend = kwargs['backend']

        self.k8s = allocator.KubernetesAllocator()

    def set_resource_expiry(self, resource_uuid):
        expired = (datetime.datetime.now() + datetime.timedelta(seconds=self.lifetime)).timestamp()

        self.resources[resource_uuid] = expired

        logger.info(f'Set resource {resource_uuid!r} to expire in {self.lifetime!r} seconds')

    def try_extend_resource_expiry(self, resource_uuid):
        if resource_uuid in self.resources:
            logger.info(f'Extending resource {resource_uuid!r} expiration')

            self.set_resource_expiry(resource_uuid)

            return True

        return False

    def allocate_resources(self, request_raw):
        """ Allocate resources.

        Only support the following resources types: Pod, Deployment, Service, Ingress.

        Args:
            request: A str containing a list of YAML definition of k8s resources.
        """
        resource_uuid = hashlib.sha256(request_raw).hexdigest()[:7]

        labels = {'compute.io/resource-group': resource_uuid}

        namespace = NAMESPACE

        request = json.loads(request_raw)

        logger.info(f'Allocating {len(request)!r} resources with uuid {resource_uuid!r}')

        service_account_name = SA_NAME

        image_pull_secret = IMAGE_PULL_SECRET

        existing = self.try_extend_resource_expiry(resource_uuid)

        if not existing:
            try:
                self.k8s.create_resources(request, namespace, labels, service_account_name, image_pull_secret)
            except client.rest.ApiException as e:
                logger.exception(f'Kubernetes API call failed status code {e.status!r}')

                # 403 is Forbidden tends to be raised when namespace is out of resources.
                # 409 is Conflict, generally because the resource already exists.
                if e.status not in (403, 409):
                    raise ResourceAllocationError()

                raise Exception()

            self.set_resource_expiry(resource_uuid)

        extra = {
            'namespace': namespace,
        }

        return extra

    def on_event(self, address, transition, *args):
        logger.info(f'{self!s} transition state {transition!r}')

        if transition == constants.RESOURCE:
            try:
                extra = self.allocate_resources(args[0])
            except Exception as e:
                new_frames = [address, constants.ERR, str(e).encode()]

                logger.info(f'Notifying backend {address} of allocation error')
            else:
                new_frames = [address, constants.ACK, json.dumps(extra).encode()]

                logger.info(f'Notifying backend {address} of allocation success')

            try:
                self.backend.send_multipart(new_frames)
            except zmq.Again:
                logger.error(f'Error notifying backend of successful allocation')

            return None

        return self


class Worker(object):
    def __init__(self, address, version, **kwargs):
        self.address = address
        self.version = version
        self.frames = None
        self.expiry = time.time() + constants.HEARTBEAT_INTERVAL * constants.HEARTBEAT_LIVENESS
        self.state = WaitingResourceRequestState(**kwargs)

    def on_event(self, address, *frames):
        old_state = str(self.state)

        with metrics.PROVISIONER_STATE_PROCESS_DURATION.labels(old_state).time():
            self.state = self.state.on_event(address, *frames)

        logger.info(f'Worker {address!r} transition from {old_state!s} to {self.state!s}')

        if self.state is None:
            raise RemoveWorkerError()

class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

        self.heartbeat_at = time.time() + constants.HEARTBEAT_INTERVAL

    def ready(self, worker):
        self.queue.pop(worker.address, None)

        self.queue[worker.address] = worker

        logger.info('Setting worker %r to expire at %r', worker.address, worker.expiry)

    def remove(self, address):
        self.queue.pop(address)

        logger.info(f'Removed worker {address!r}')

    def purge(self):
        t = time.time()

        expired = []

        for address, worker in self.queue.items():
            if t > worker.expiry:
                expired.append(address)

        for address in expired:
            self.queue.pop(address, None)

            logger.info('Idle worker %r expired', address)

    @property
    def heartbeat_time(self):
        return time.time() >= self.heartbeat_at

    def heartbeat(self, socket):
        for address in self.queue:
            try:
                socket.send_multipart([address, constants.HEARTBEAT])

                logger.debug('Send heartbeat to %r', address)
            except zmq.Again:
                logger.error('Error sending heartbaet to %r', address)

        self.heartbeat_at = time.time() + constants.HEARTBEAT_INTERVAL

        logger.debug('Setting next heartbeat at %r', self.heartbeat_at)

    def next(self, version):
        candidates = [x.address for x in self.queue.values() if x.version == version]

        logger.info('Candidates for next worker handling version %r: %r', version, candidates)

        address = candidates.pop(0)

        self.queue.pop(address, None)

        return address


class Provisioner(threading.Thread):
    def __init__(self, lifetime, **kwargs):
        super(Provisioner, self).__init__(target=self.monitor)

        self.lifetime = lifetime or LIFETIME

        self.context = None

        self.frontend = None

        self.backend = None

        self.poll_both = None

        self.workers = WorkerQueue()

        self.running = {}

        kwargs = {
            'namespace': kwargs['namespace'] or NAMESPACE,
            'timeout': kwargs['timeout'] or TIMEOUT,
            'dry_run': kwargs['dry_run'],
            'ignore_lifetime': kwargs['ignore_lifetime'],
        }

        self.kube_cluster = kube_cluster.KubeCluster(**kwargs)

    def initialize(self, frontend_port, backend_port):
        """ Initializes the load balancer.

        We initialize the zmq context, create the frontend/backend sockets and
        creates their respective pollers.

        Args:
            frontend_port: An int port number used to listen for frontend
                connections.
            backend_port: An int port number used to listen for backend
                connections.
        """
        self.context = zmq.Context(1)

        self.frontend = self.context.socket(zmq.ROUTER)

        FSNDTIMEO = os.environ.get('FRONTEND_SEND_TIMEOUT', 15)
        FRCVTIMEO = os.environ.get('FRONTEND_RECV_TIMEOUT', 15)

        self.frontend.setsockopt(zmq.SNDTIMEO, FSNDTIMEO * 1000)
        self.frontend.setsockopt(zmq.RCVTIMEO, FRCVTIMEO * 1000)

        frontend_addr = 'tcp://*:{!s}'.format(frontend_port)

        self.frontend.bind(frontend_addr)

        logger.info('Binding frontend to %r', frontend_addr)

        self.backend = self.context.socket(zmq.ROUTER)

        BSNDTIMEO = os.environ.get('BACKEND_SEND_TIMEOUT', 15)
        BRCVTIMEO = os.environ.get('BACKEND_RECV_TIMEOUT', 15)

        self.backend.setsockopt(zmq.SNDTIMEO, BSNDTIMEO * 1000)
        self.backend.setsockopt(zmq.RCVTIMEO, BRCVTIMEO * 1000)

        backend_addr = 'tcp://*:{!s}'.format(backend_port)

        self.backend.bind(backend_addr)

        logger.info('Binding backend to %r', backend_addr)

        self.poll_both = zmq.Poller()

        self.poll_both.register(self.frontend, zmq.POLLIN)

        self.poll_both.register(self.backend, zmq.POLLIN)

    def handle_frontend_frames(self, frames, queue):
        logger.info('Handling frontend frames %r', frames)

        # Address and blank space
        address = frames[0:2]

        version = frames[2]

        # Frames to be forwarded to the backend
        frames = frames[2:]

        queue.append([version]+frames)

        metrics.PROVISIONER_QUEUE.inc()
        metrics.PROVISIONER_QUEUED.inc()

        response = address + [constants.ACK]

        logger.info('Successfully queued job')

        try:
            self.frontend.send_multipart(response)
        except zmq.Again:
            logger.info('Error notifying client with response %r', response)
        else:
            logger.info('Notified client with response %r', response)

    def handle_backend_frames(self, frames, queue, resources):
        address = frames[0]

        logger.info('Handling frames from backend %r', frames[:3])

        if frames[1] in (constants.READY, constants.HEARTBEAT):
            version = frames[2]

            kwargs = {
                'namespace': 'default',
                'lifetime': self.lifetime,
                'backend': self.backend,
                'queue': queue,
                'resources': resources
            }

            self.workers.ready(Worker(address, version, **kwargs))
        else:
            try:
                self.running[address].on_event(*frames)
            except KeyError:
                logger.error(f'Worker {address!r} is not in the running list')
            except RemoveWorkerError:
                self.running.pop(address)
            except ResourceAllocationError:
                self.running.pop(address)

                queue.insert(0, [version]+frames[3:])

                logger.info(f'Worker {address!r} failed, requeueing job')

    def dispatch_workers(self, queue):
        if len(queue) == 0:
            return

        for address, worker in list(self.workers.queue.items()):
            logger.info('Processing waiting worker %r', address)

            frames = queue.pop(0)

            version = frames.pop(0)

            frames.insert(0, address)

            frames.insert(1, constants.REQUEST)

            try:
                self.backend.send_multipart(frames)
            except zmq.Again:
                logger.info('Error sending frames to worker %r', address)

                queue.insert(0, [version]+frames[2:])
            else:
                metrics.PROVISIONER_QUEUE.dec()

                worker.frames = raw_frames

                self.running[address] = worker

                self.workers.remove(address)

                logger.info(f'Moved worker {address!r} to running')

    def monitor(self):
        """ Main loop for the load balancer.
        """
        self.initialize(FRONTEND_PORT, BACKEND_PORT)

        queue = []
        resources = {}

        while True:
            socks = dict(self.poll_both.poll(constants.HEARTBEAT_INTERVAL * 1000))

            if socks.get(self.backend) == zmq.POLLIN:
                try:
                    frames = self.backend.recv_multipart()
                except zmq.Again:
                    logger.info('Error receiving frames from backend')
                else:
                    if not frames:
                        break

                    self.handle_backend_frames(frames, queue, resources)

            if socks.get(self.frontend) == zmq.POLLIN:
                try:
                    frames = self.frontend.recv_multipart()
                except zmq.Again:
                    logger.info('Error receiving frames from frontend')
                else:
                    if not frames:
                        break

                    self.handle_frontend_frames(frames, queue)

            if self.workers.heartbeat_time:
                self.workers.heartbeat(self.backend)

            self.workers.purge()

            self.dispatch_workers(queue)

            self.kube_cluster.check_resources(resources)


def create_resources():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('input_dir')

    args = parser.parse_args()

    data = []

    for x in os.listdir(args.input_dir):
        with open(os.path.join(args.input_dir, x)) as f:
            data.append(f.read())

    k8s = KubernetesAllocator()

    k8s.create_resources(data, 'default', {'compute.io/group-uid': 'test'}, 'compute', 'docker-registry')


def delete_resources():
    k8s = KubernetesAllocator()

    k8s.delete_resources('default', 'compute.io/group-uid=test')


def main():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level',
                        help='Logging level',
                        choices=logging._nameToLevel.keys())

    parser.add_argument('--lifetime',
                        help='Life time of resources in seconds',
                        type=int)

    parser.add_argument('--namespace',
                        help='Kubernetes namespace to monitor')

    parser.add_argument('--dry-run',
                        help='Does not actually remove resources',
                        action='store_true')

    parser.add_argument('--ignore-lifetime',
                        help='Ignores lifetime',
                        action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level or LOG_LEVEL)

    start_http_server(8888, '0.0.0.0', registry=metrics.registry)

    logger.info('Started metrics server')

    provisioner = Provisioner(**vars(args))

    provisioner.start()

    logger.info('Started provisioner')

    provisioner.join()

    logger.info('Thread exited')

if __name__ == '__main__':
    main()
