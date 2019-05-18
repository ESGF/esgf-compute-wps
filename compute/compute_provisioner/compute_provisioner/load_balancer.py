import os
import logging

import zmq

FRONTEND_PORT = os.environ.get('FRONTEND_PORT', 7777)
BACKEND_PORT = os.environ.get('BACKEND_PORT', 7778)

logger = logging.getLogger('compute_provisioner.provisioner')


class LoadBalancer(object):
    def __init__(self):
        self.context = None

        self.frontend = None

        self.backend = None

        self.poll_workers = None

        self.poll_both = None

        self.workers = {}

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

        logger.info('Created zmq context')

        self.frontend = self.context.socket(zmq.ROUTER)

        self.frontend.bind('tcp://*:{!s}'.format(frontend_port))

        logger.info('Created and bound frontend socket')

        self.backend = self.context.socket(zmq.ROUTER)

        self.backend.bind('tcp://*:{!s}'.format(backend_port))

        logger.info('Created and bound backend socket')

        self.poll_workers = zmq.Poller()

        self.poll_workers.register(self.backend, zmq.POLLIN)

        logger.info('Registered worker poller')

        self.poll_both = zmq.Poller()

        self.poll_both.register(self.frontend, zmq.POLLIN)

        self.poll_both.register(self.backend, zmq.POLLIN)

        logger.info('Registered client and worker poller')

    def run(self):
        """ Main loop for the load balancer.

        The workflow is described as follows. The worker should initializ a REQ
        connection to the load balancer, it should send a multipart message;
        the first value is READY and the second is the version. This registers
        that a worker is ready to receive work for a specific version. Next the
        client should initialize a REQ connection to the frontend of the load
        balancer and send a multipart message; the first value is the
        datainputs and the second value is the specific version. Next the
        message is forwarded to an available backend, the backend receives a
        message with the following values; first client identitier, second is
        blank, third is the specific version, and the rest is the data passed
        to the backend. The worker should return the exact message when the
        work is done, extra data can be appended. Finally the message is
        forwarded back to the client.
        """
        while True:
            if self.workers:
                logger.info('Polling both')

                socks = dict(self.poll_both.poll())
            else:
                logger.info('Polling workers')

                socks = dict(self.poll_workers.poll())

            if socks.get(self.backend) == zmq.POLLIN:
                # backend response
                msg = self.backend.recv_multipart()

                logger.info('Received %r from backend', msg)

                if not msg:
                    logger.info('Did not receive a message')

                    break

                # Should be a version string
                version = msg[3]

                if version in self.workers:
                    self.workers[version].append(msg[0])
                else:
                    self.workers[version] = [msg[0]]

                # Worker is ready to accept work
                if msg[2] != b'READY':
                    logger.info('Sending frontend reply %r', msg[2:])

                    # frontend reply
                    self.frontend.send_multipart(msg[2:])

            if socks.get(self.frontend) == zmq.POLLIN:
                # frontend request
                msg = self.frontend.recv_multipart()

                logger.info('Received %r from frontend', msg)

                version = msg[2]

                # TODO We can check if a worker for the appropriate version is
                # available. If not, we can dynamically allocate the resource
                # in the cluster and then foward the job. This might involve
                # adding a queue for the jobs waiting for their resources to be
                # allocated.
                request = [self.workers[version].pop(0), b''] + msg

                logger.info('Sending backend request %r', request)

                # backend reply
                self.backend.send_multipart(request)


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO)

    load_balancer = LoadBalancer()

    load_balancer.initialize(FRONTEND_PORT, BACKEND_PORT)

    load_balancer.run()
