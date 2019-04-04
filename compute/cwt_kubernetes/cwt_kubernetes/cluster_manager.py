import logging
import time

from dask.distributed import Client

METRIC_KEYS = ('executing', 'in_memory', 'ready', 'in_flight')


class ClusterManager(object):
    def __init__(self, scheduler, cluster):
        self.scheduler = scheduler

        self.client = Client(scheduler)

        self.cluster = cluster

        self.running = False

    def find_stale_workers(self):
        candidates = []

        try:
            workers = self.client.scheduler_info()['workers']
        except KeyError:
            logging.error('Scheduler info was missing workers key')

            raise
        except Exception as e:
            logging.error('Error retrieving scheduler info: %r', e)

            raise

        try:
            for address, state in list(workers.items()):
                metric = sum(state['metrics'][x] for x in METRIC_KEYS)

                if metric == 0:
                    candidates.append(state['host'])
        except KeyError:
            logging.error('Malformmed scheduler info')

            raise

        return candidates

    def run(self):
        self.running = True

        while self.running:
            try:
                stale_workers = self.find_stale_workers()
            except Exception as e:
                logging.error('Failed to find stale workers: %r', e)

            try:
                self.cluster.scale_down(stale_workers)
            except Exception as e:
                logging.error('Failed to scale down dask workers: %r', e)

            time.sleep(10)
