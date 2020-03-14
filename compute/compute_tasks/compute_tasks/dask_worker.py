import itertools
import logging
import os
from collections.abc import MutableMapping
from functools import partial

import redis
import zict
from distributed import worker
from distributed import protocol
from distributed.sizeof import safe_sizeof

REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_DB = os.environ.get('REDIS_DB', '0')

logger = logging.getLogger(__name__)

class LocalStore(MutableMapping):
    """ Local in-memory store.
    """
    def __init__(self):
        self.data = {}

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.data[key] = value

    def __delitem__(self, key):
        del self.data[key]

    def __iter__(self):
        for x in self.data:
            yield x

    def __len__(self):
        return len(self.data)

class CachedStore(MutableMapping):
    """ Cache store for dask worker.

    This store provides the same functionality as the default store; fill memory to limit then spill to disk,
    The added benefit is when data is remove it is pushed to a larger secondary store backed by a redis server.

    Args:
        target_memory (int): Number of bytes the store is able to hold before spilling to disk.
        storage_path (str): The path to spill data to when memories full.
    """
    def __init__(self, worker, target_memory, storage_path):
        self.worker = worker
        # Build exact chain that dask already provides,
        # Fill local memory to target then spill to disk
        file = zict.File(storage_path)
        self.slow = zict.Func(partial(protocol.serialize_bytelist, on_error='raise'), protocol.deserialize_bytes, file)

        self.fast = LocalStore()

        self.l1 = zict.Buffer(self.fast, self.slow, target_memory, lambda x, y: safe_sizeof(y))

        logger.info(f'Setting fast store memory limit {target_memory} bytes and disk spill path {storage_path}')

        self.l2 = redis.client.Redis(REDIS_HOST, int(REDIS_PORT), int(REDIS_DB))

    def __getitem__(self, key):
        try:
            data = self.l1[key]
        except KeyError:
            logger.warning(f'Key {key} not in local store')
        else:
            logger.info(f'Found {key} in local store')

            return data

        data = self.l2[key]

        nbytes = self.worker.nbytes[key] = safe_sizeof(data)

        types = self.worker.types[key] = type(data)

        logger.info(f'Found {key} in redis store, nbytes {nbytes}, types {types}')

        return protocol.deserialize_bytes(data)

    def __setitem__(self, key, value):
        self.l1[key] = value

    def __delitem__(self, key):
        try:
            data = self.l1[key]
        except KeyError:
            logger.error(f'Key {key} was not found in local store')
        else:
            del self.l1[key]

            try:
                self.l2[key] = protocol.serialize_bytes(data)
            except redis.exceptions.ResponseError:
                info = self.l2.info('memory')

                logger.error('OOM response from redis trying to store {sizeof(data)} {info["used_memoryy"]} of {info["maxmemory"]}')
            else:
                nbytes = self.worker.nbytes[key] = safe_sizeof(data)

                types = self.worker.types[key] = type(data)

                logger.info(f'Moved {key} from local memory to redis store, nbytes {nbytes}, types {types}')

    def __iter__(self):
        return itertools.chain(iter(self.l1), iter(self.l2.keys()))

    def __len__(self):
        return len(self.l1) + len(self.l2.keys())

class CachedWorker(worker.Worker):
    """ Dask worker subclass.

    Replaces standard store with a Cached storage supported by Redis, see `compute_tasks.dask_worker.CachedStore`.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        target_memory = int(float(self.memory_limit) * self.memory_target_fraction)

        storage_path = os.path.join(self.local_directory, 'storage')

        self.data = CachedStore(self, target_memory, storage_path)
