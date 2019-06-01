import json
import logging
import time
from collections import OrderedDict

import redis
import zmq

from compute_provisioner import constants

logger = logging.getLogger('compute_provisioner.provisioner')


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

            logger.info('Idle worker expired: %r', address)

    @property
    def heartbeat_time(self):
        return time.time() >= self.heartbeat_at

    def heartbeat(self, socket):
        for address in self.queue:
            msg = [address, constants.HEARTBEAT]

            socket.send_multipart(msg)

            logger.info('Sent heartbeat to %r', address)

        self.heartbeat_at = time.time() + constants.HEARTBEAT_INTERVAL

    def next(self, version):
        candidates = [x.address for x in self.queue.values() if x.version == version]

        logger.info('Candidates %r', candidates)

        address = candidates.pop(0)

        self.queue.pop(address, None)

        return address


class LoadBalancer(object):
    def __init__(self):
        self.context = None

        self.frontend = None

        self.backend = None

        self.poll_both = None

        self.workers = WorkerQueue()

        self.redis = None

        self.waiting_ack = {}

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

        frontend_addr = 'tcp://*:{!s}'.format(frontend_port)

        self.frontend.bind(frontend_addr)

        logger.info('Binding frontend to %r', frontend_addr)

        self.backend = self.context.socket(zmq.ROUTER)

        backend_addr = 'tcp://*:{!s}'.format(backend_port)

        self.backend.bind(backend_addr)

        logger.info('Binding backend to %r', backend_addr)

        self.poll_both = zmq.Poller()

        self.poll_both.register(self.frontend, zmq.POLLIN)

        self.poll_both.register(self.backend, zmq.POLLIN)

        self.redis = redis.Redis(host=constants.REDIS_HOST, db=1)

        logger.info('Connected to redis db %r', self.redis)

    def handle_backend_frames(self, frames):
        logger.info('Handling frames from backend %r', frames)

        address = frames[0]

        version = frames[2]

        if address in self.waiting_ack:
            logger.info('Received ack from backend %r', address)

            self.waiting_ack.pop(address, None)
        else:
            self.workers.ready(Worker(address, version))

            if frames[1] not in (constants.READY, constants.HEARTBEAT):
                logger.error('Unknown message %r', frames)

    def handle_frontend_frames(self, frames):
        self.frontend.send_multipart(frames)

        logger.info('Handling frames from frontend %r', frames)

        version = frames[2]

        with self.redis.lock('job_queue'):
            self.redis.rpush(version, json_encoder(frames))

        logger.info('Added frames to %r queue', version)

    def handle_unacknowledge_requests(self):
        logger.debug('Handling unacknowledged requests')

        for address, waiting in list(self.waiting_ack.items()):
            if time.time() >= waiting.expiry:
                logger.info('Backend %r never acknowledged request', address)

                self.waiting_ack.pop(address, None)

                # Reinsert the request frames infront of the queue
                with self.redis.lock('job_queue'):
                    self.redis.lpush(waiting.version, json_encoder(waiting.frames))

    def dispatch_workers(self):
        logger.debug('Dispatching requests to workers')

        try:
            with self.redis.lock('job_queue', blocking_timeout=4):
                for address, worker in list(self.workers.queue.items()):
                    try:
                        frames = json_decoder(self.redis.lpop(worker.version))
                    except Exception as e:
                        logger.error('Failed to pop job from queue: %r', e)

                        continue

                    frames.insert(0, address)

                    self.backend.send_multipart(frames)

                    self.waiting_ack[address] = WaitingAck(worker.version, frames[1:])

                    logger.info('Waiting for ack from backend %r', address)

                    # Remove the worker
                    self.workers.queue.pop(address, None)
        except redis.lock.LockError:
            pass

    def run(self):
        """ Main loop for the load balancer.
        """
        while True:
            socks = dict(self.poll_both.poll(constants.HEARTBEAT_INTERVAL * 1000))

            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()

                if not frames:
                    break

                self.handle_backend_frames(frames)

            if socks.get(self.frontend) == zmq.POLLIN:
                frames = self.frontend.recv_multipart()

                if not frames:
                    break

                self.handle_frontend_frames(frames)

            if self.workers.heartbeat_time:
                self.workers.heartbeat(self.backend)

            self.workers.purge()

            self.dispatch_workers()

            self.handle_unacknowledge_requests()


def main():
    import logging

    logging.basicConfig(level=logging.INFO)

    load_balancer = LoadBalancer()

    load_balancer.initialize(constants.FRONTEND_PORT, constants.BACKEND_PORT)

    load_balancer.run()


if __name__ == '__main__':
    main()
