import datetime
import hashlib
import json
import logging
import os
import threading
import time
from collections import OrderedDict
from uuid import uuid4

import redis
import yaml
import zmq
from kubernetes import client
from kubernetes import config

from compute_provisioner import constants
from compute_provisioner import kube_cluster

logger = logging.getLogger('provisioner')

CLEANUP_PHASES = ('Succeeded', 'Failed')


class ResourceTimeoutError(Exception):
    pass


class ResourceAllocationError(Exception):
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


class WaitingAck(object):
    def __init__(self, version, frames):
        self.version = version
        self.frames = frames
        self.expiry = time.time() + constants.ACK_TIMEOUT


class Worker(object):
    def __init__(self, address, version):
        self.address = address
        self.version = version
        self.expiry = time.time() + constants.HEARTBEAT_INTERVAL * constants.HEARTBEAT_LIVENESS


class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

        self.heartbeat_at = time.time() + constants.HEARTBEAT_INTERVAL

    def ready(self, worker):
        self.queue.pop(worker.address, None)

        self.queue[worker.address] = worker

        logger.info('Setting worker %r to expire at %r', worker.address, worker.expiry)

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
                logger.info('Error sending heartbaet to %r', address)

        self.heartbeat_at = time.time() + constants.HEARTBEAT_INTERVAL

        logger.debug('Setting next heartbeat at %r', self.heartbeat_at)

    def next(self, version):
        candidates = [x.address for x in self.queue.values() if x.version == version]

        logger.info('Candidates for next worker handling version %r: %r', version, candidates)

        address = candidates.pop(0)

        self.queue.pop(address, None)

        return address


class Provisioner(threading.Thread):
    def __init__(self, frontend_port, backend_port, redis_host, namespace, lifetime, resource_timeout, **kwargs):
        super(Provisioner, self).__init__(target=self.monitor, args=(frontend_port, backend_port, redis_host))
        self.namespace = namespace

        self.lifetime = lifetime

        self.resource_timeout = resource_timeout

        self.context = None

        self.frontend = None

        self.backend = None

        self.poll_both = None

        self.workers = WorkerQueue()

        self.redis = None

        self.waiting_ack = {}

        config.load_incluster_config()

        self.core = client.CoreV1Api()

        self.apps = client.AppsV1Api()

        self.extensions = client.ExtensionsV1beta1Api()

    def initialize(self, frontend_port, backend_port, redis_host):
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

        self.redis = redis.Redis(host=redis_host, db=1)

        logger.info('Connected to redis db %r', self.redis)

    def queue_job(self, version, frames):
        self.redis.rpush(version, json_encoder(frames))

        logger.info('Queued job')

    def requeue_job(self, version, frames):
        self.redis.lpush(version, json_encoder(frames))

        logger.info('Requeued job')

    def update_labels(self, yaml_data, labels):
        try:
            yaml_data['metadata']['labels'].update(labels)
        except AttributeError:
            # Labels not a dict, is this really a possible case?
            yaml_data['metadata']['labels'] = labels
        except KeyError:
            # Labels does not exists
            yaml_data['metadata'] = {
                'labels': labels,
            }

    def request_resources(self, request, resource_uuid):
        labels = {
            'app.kubernetes.io/resource-group-uuid': resource_uuid,
        }

        try:
            for item in request:
                yaml_data = yaml.safe_load(item)

                self.update_labels(yaml_data, labels)

                kind = yaml_data['kind']

                if kind == 'Pod':
                    self.core.create_namespaced_pod(body=yaml_data, namespace=self.namespace)
                elif kind == 'Deployment':
                    self.apps.create_namespaced_deployment(body=yaml_data, namespace=self.namespace)
                elif kind == 'Service':
                    self.core.create_namespaced_service(body=yaml_data, namespace=self.namespace)
                elif kind == 'Ingress':
                    self.extensions.create_namespaced_ingress(body=yaml_data, namespace=self.namespace)
                else:
                    raise Exception('Requested an unsupported resource')
        except client.rest.ApiException as e:
            logger.info(f'Kubernetes API call failed status code {e.status!r}')

            # 403 is Forbidden tends to be raised when namespace is out of resources.
            # 409 is Conflict, generally because the resource already exists.
            if e.status not in (403, 409):
                raise ResourceAllocationError()

    def set_resource_expiry(self, resource_uuid):
        expired = (datetime.datetime.now() + datetime.timedelta(seconds=self.lifetime)).timestamp()

        self.redis.hset('resource', resource_uuid, expired)

        logger.info(f'Set resource {resource_uuid!r} to expire in {self.lifetime!r} seconds')

    def try_extend_resource_expiry(self, resource_uuid):
        if self.redis.hexists('resource', resource_uuid):
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

        request = json.loads(request_raw)

        logger.info(f'Allocating {len(request)!r} resources with uuid {resource_uuid!r}')

        # Check if the resources exist
        existing = self.try_extend_resource_expiry(resource_uuid)

        if not existing:
            try:
                self.request_resources(request, resource_uuid)
            except ResourceAllocationError as e:
                raise e
            except Exception:
                pass

            self.set_resource_expiry(resource_uuid)

    def handle_backend_frames(self, frames):
        address = frames[0]

        logger.info('Handling frames from backend %r', frames[:3])

        if frames[1] == constants.RESOURCE:
            try:
                self.allocate_resources(frames[2])
            except (ResourceAllocationError, ResourceTimeoutError) as e:
                waiting = self.waiting_ack.pop(address, None)

                if waiting is not None:
                    # On resource allocation error job is pushed to front of the queue.
                    with self.redis.lock('job_queue'):
                        self.redis.lpush(waiting.version, json_encoder(waiting.frames))
                else:
                    logger.error('Failed allocation but job is missing from queue waiting for ack')
            else:
                new_frames = [address, constants.ACK]

                logger.info('Notifying backend %r of successful allocation', address)

                try:
                    self.backend.send_multipart(new_frames)
                except zmq.Again:
                    # If the ack never sends we just let the worker timeout
                    pass
                else:
                    logger.info('Removing worker %s from waiting ack', address)

                    self.waiting_ack.pop(address, None)
        else:
            version = frames[2]

            self.workers.ready(Worker(address, version))

            if frames[1] == constants.READY:
                logger.info('Received ready message from backend %r', address)

                pass
            elif frames[1] == constants.HEARTBEAT:
                logger.debug('Received heartbeat from backend %r', address)

                pass
            else:
                logger.error('Unknown message %r', frames)

    def handle_frontend_frames(self, frames):
        logger.info('Handling frontend frames %r', frames)

        # Address and blank space
        address = frames[0:2]

        version = frames[2]

        # Frames to be forwarded to the backend
        frames = frames[2:]

        try:
            self.queue_job(version, frames)
        except redis.lock.LockError:
            response = address + [constants.ERR]

            logger.info('Error aquiring redis lock')
        else:
            response = address + [constants.ACK]

            logger.info('Successfully queued job')

        try:
            self.frontend.send_multipart(response)
        except zmq.Again:
            logger.info('Error notifying client with response %r', response)
        else:
            logger.info('Notified client with response %r', response)

    def handle_unacknowledge_requests(self):
        for address, waiting in list(self.waiting_ack.items()):
            if time.time() >= waiting.expiry:
                logger.info('Backend %r never acknowledged request', address)

                self.waiting_ack.pop(address, None)

                # Reinsert the request frames infront of the queue
                with self.redis.lock('job_queue'):
                    self.redis.lpush(waiting.version, json_encoder(waiting.frames))

    def dispatch_workers(self):
        try:
            with self.redis.lock('job_queue', blocking_timeout=4):
                for address, worker in list(self.workers.queue.items()):
                    logger.info('Processing waiting worker %r', address)

                    try:
                        frames = json_decoder(self.redis.lpop(worker.version))
                    except Exception:
                        # TODO handle edge case of malformed data
                        logger.info('No work found for version %r', worker.version)

                        continue

                    frames.insert(0, address)

                    frames.insert(1, constants.REQUEST)

                    try:
                        self.backend.send_multipart(frames)
                    except zmq.Again:
                        logger.info('Error sending frames to worker %r', address)

                        self.redis.lpush(worker.version, json_encoder(frames[2:]))
                    else:
                        logger.info('Sent frames to worker %r waiting for acknowledgment', address)

                    self.waiting_ack[address] = WaitingAck(worker.version, frames[2:])

                    # Remove the worker
                    self.workers.queue.pop(address, None)

                    logger.info('Dispatch work to %r', address)
        except redis.lock.LockError:
            # No need to handle anything if we fail to aquire the lock
            # TODO we should track how often we cannot aquire the lock, as there may be a bigger issue
            pass

    def monitor(self, frontend_port, backend_port, redis_host):
        """ Main loop for the load balancer.
        """
        self.initialize(frontend_port, backend_port, redis_host)

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

                    self.handle_backend_frames(frames)

            if socks.get(self.frontend) == zmq.POLLIN:
                try:
                    frames = self.frontend.recv_multipart()
                except zmq.Again:
                    logger.info('Error receiving frames from frontend')
                else:
                    if not frames:
                        break

                    self.handle_frontend_frames(frames)

            if self.workers.heartbeat_time:
                self.workers.heartbeat(self.backend)

            self.workers.purge()

            self.dispatch_workers()

            self.handle_unacknowledge_requests()


def main():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--redis-host', help='Redis host', required=True)

    parser.add_argument('--frontend-port', help='Frontend port', type=int, default=7777)

    parser.add_argument('--backend-port', help='Backend port', type=int, default=7778)

    parser.add_argument('--namespace', help='Kubernetes namespace to monitor', default='default')

    parser.add_argument('--timeout', help='Resource monitor timeout', type=int, default=30)

    parser.add_argument('--resource-timeout', help='Time in seconds allowed for resources to be allocated', type=int,
                        default=240)

    parser.add_argument('--lifetime', help='Time in seconds to let resources live', type=int, default=3600)

    parser.add_argument('--ignore-lifetime', help='Ignores lifetime and removes existing resources', type=bool, default=False)

    parser.add_argument('--enable-resource-monitor', help='Enables monitoring expired resources', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    if args.enable_resource_monitor:
        logger.info('Starting resource monitor')

        monitor = kube_cluster.KubeCluster(args.redis_host, args.namespace, args.timeout, False, args.ignore_lifetime)

        monitor.start()

        monitor.join()

    provisioner = Provisioner(**vars(args))

    provisioner.start()

    provisioner.join()


if __name__ == '__main__':
    main()
