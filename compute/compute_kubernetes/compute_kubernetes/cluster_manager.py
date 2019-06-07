import logging
import time

from dask.distributed import Client

METRIC_KEYS = ('executing', 'in_memory', 'ready', 'in_flight')


class ClusterManager(object):
    def __init__(self, scheduler, cluster, **kwargs):
        self.scheduler = scheduler

        self.client = Client(scheduler)

        self.cluster = cluster

        self.running = False

        self.timeout = kwargs.get('timeout', 30)

    def get_pods_to_kill(self):
        workers = self.client.retire_workers()

        try:
            return [x['host'] for x in workers.values()]
        except AttributeError:
            return []

    def scale_up_workers(self, max_n, inc_n=None, labels=None):
        if labels is None:
            labels = {}

        existing = self.cluster.get_pods(labels)

        logging.info('Found %r existing workers with labels %r', len(existing.items), labels)

        # For the time being always scale up to max_n eventually we can
        # make this smarter and have it look at the saturation of the
        # workers
        if len(existing.items) < max_n:
            to_create = max_n - len(existing.items)

            logging.info('Need to allocated %r additional workers', to_create)

            self.cluster.scale_up(to_create, labels)

    def scale_down_workers(self):
        pods = self.get_pods_to_kill()

        logging.info('Attempting to scale down by %r workers', len(pods))

        try:
            self.cluster.scale_down(pods)
        except Exception as e:
            logging.error('Failed to scale down dask workers: %r', e)

    def run(self):
        self.running = True

        while self.running:
            self.scale_down_workers()

            time.sleep(self.timeout)
