import json
import logging
import time
from collections import OrderedDict

import redis
import zmq

from compute_provisioner import constants
from compute_provisioner import metrics

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

        logger.debug('Setting worker %r to expire at %r', worker.address, worker.expiry)

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
            socket.send_multipart([address, constants.HEARTBEAT])

            logger.debug('Sent heartbeat to %r', address)

        self.heartbeat_at = time.time() + constants.HEARTBEAT_INTERVAL

        logger.debug('Setting next heartbeat at %r', self.heartbeat_at)

    def next(self, version):
        candidates = [x.address for x in self.queue.values() if x.version == version]

        logger.debug('Candidates for next worker handling version %r: %r', version, candidates)

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

        self.metrics = metrics.Metrics()

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

        self.redis = redis.Redis(host=redis_host, db=1)

        logger.info('Connected to redis db %r', self.redis)

    def handle_backend_frames(self, frames):
        address = frames[0]

        self.metrics.inc('recv_backend')

        if address in self.waiting_ack:
            logger.info('Received ack from backend %r', address)

            self.waiting_ack.pop(address, None)

            self.metrics.inc('recv_ack')
        else:
            logger.debug('Received message from backend %r', address)

            version = frames[2]

            self.workers.ready(Worker(address, version))

            if frames[1] == constants.READY:
                self.metrics.inc('recv_ready')
            elif frames[1] == constants.HEARTBEAT:
                self.metrics.inc('recv_heartbeat')
            else:
                self.metrics.inc('recv_unknown')

                logger.error('Unknown message %r', frames)

    def handle_frontend_frames(self, frames):
        self.frontend.send_multipart(frames)

        version = frames[2]

        with self.redis.lock('job_queue'):
            self.redis.rpush(version, json_encoder(frames))

        logger.info('Added frames to %r queue %r', version, frames)

        self.metrics.inc('recv_frontend')

    def handle_unacknowledge_requests(self):
        for address, waiting in list(self.waiting_ack.items()):
            if time.time() >= waiting.expiry:
                self.metrics.inc('unack')

                logger.info('Backend %r never acknowledged request', address)

                self.waiting_ack.pop(address, None)

                # Reinsert the request frames infront of the queue
                with self.redis.lock('job_queue'):
                    self.redis.lpush(waiting.version, json_encoder(waiting.frames))

    def dispatch_workers(self):
        try:
            with self.redis.lock('job_queue', blocking_timeout=4):
                for address, worker in list(self.workers.queue.items()):
                    logger.debug('Processing waiting worker %r', address)

                    try:
                        frames = json_decoder(self.redis.lpop(worker.version))
                    except Exception:
                        logger.debug('No work found for version %r', worker.version)

                        continue

                    frames.insert(0, address)

                    self.backend.send_multipart(frames)

                    self.waiting_ack[address] = WaitingAck(worker.version, frames[1:])

                    # Remove the worker
                    self.workers.queue.pop(address, None)

                    logger.info('Dispatch work to %r', address)

                    self.metrics.inc('dispatched')
        except redis.lock.LockError:
            pass

    def run(self, frontend_port, backend_port, redis_host):
        """ Main loop for the load balancer.
        """
        self.initialize(frontend_port, backend_port, redis_host)

        while True:
            socks = dict(self.poll_both.poll(constants.HEARTBEAT_INTERVAL * 1000))

            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()

                if not frames:
                    self.metrics.inc('recv_backend_empty')

                    break

                self.handle_backend_frames(frames)

            if socks.get(self.frontend) == zmq.POLLIN:
                frames = self.frontend.recv_multipart()

                if not frames:
                    self.metrics.inc('recv_frontend_empty')

                    break

                self.handle_frontend_frames(frames)

            if self.workers.heartbeat_time:
                self.workers.heartbeat(self.backend)

                self.metrics.inc('sent_heartbeat')

            self.workers.purge()

            self.dispatch_workers()

            self.handle_unacknowledge_requests()


def main():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', help='Logging level', choices=logging._nameToLevel.keys(), default='INFO')

    parser.add_argument('--redis-host', help='Redis host')

    parser.add_argument('--frontend-port', help='Frontend port')

    parser.add_argument('--backend-port', help='Backend port')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level)

    load_balancer = LoadBalancer()

    frontend_port = args.frontend_port or constants.FRONTEND_PORT

    backend_port = args.backend_port or constants.BACKEND_PORT

    redis_host = args.redis_host or constants.REDIS_HOST

    load_balancer.run(frontend_port, backend_port, redis_host)


if __name__ == '__main__':
    main()
