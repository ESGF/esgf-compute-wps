import time
import logging

logger = logging.getLogger(__name__)

class RetryExceptionWrapper(Exception):
    def __init__(self, e):
        self.e = e

def retry(count, delay, filter=None):
    def wrapper(func):
        def wrapped(*args, **kwargs):
            retry_delay = delay

            last_exc = None

            for x in range(count):
                try:
                    data = func(*args, **kwargs)
                except Exception as e:
                    logger.info(f'Caught exception {type(e)}')

                    if filter is not None and filter(e):
                        raise RetryExceptionWrapper(e)

                    last_exc = e
                else:
                    return data

                logger.info(f'Retrying after {retry_delay} caught {last_exc}')

                time.sleep(retry_delay)

                retry_delay = retry_delay * 2  # noqa F841, F823

            raise last_exc
        return wrapped
    return wrapper
