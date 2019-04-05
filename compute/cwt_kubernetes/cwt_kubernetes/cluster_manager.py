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
