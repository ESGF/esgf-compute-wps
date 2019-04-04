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

        self.timeout = kwargs.get('timeout', 60)

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

        logging.info('Returned %r workers', len(workers))

        try:
            for address, state in list(workers.items()):
                metric = sum(state['metrics'][x] for x in METRIC_KEYS)

                if metric == 0:
                    candidates.append(state['host'])
        except KeyError:
            logging.error('Malformmed scheduler info')

            raise

        logging.info('Found %r candidates for removal', len(candidates))

        return candidates

    def run(self):
        self.running = True

        while self.running:
            try:
                stale_workers = self.find_stale_workers()
            except Exception as e:
                logging.error('Failed to find stale workers: %r', e)

            logging.info('Attempting to scale down by %r workers', len(stale_workers))

            try:
                self.cluster.scale_down(stale_workers)
            except Exception as e:
                logging.error('Failed to scale down dask workers: %r', e)

            time.sleep(self.timeout)
