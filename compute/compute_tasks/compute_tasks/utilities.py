import time
import logging

logger = logging.getLogger(__name__)

def retry(count, delay, raise_errors=None):
    if raise_errors is None:
        raise_errors = ()

    def wrapper(func):
        def wrapped(*args, **kwargs):
            retry_delay = delay

            last_exc = None

            for x in range(count):
                try:
                    data = func(*args, **kwargs)
                except Exception as e:
                    logger.info('HELP %r', e)

                    if len(raise_errors) > 0 and isinstance(e, raise_errors):
                        raise e

                    last_exc = e
                else:
                    return data

                logger.debug('Delaying retry by %r seconds', delay)

                time.sleep(retry_delay)

                retry_delay = retry_delay * 2  # noqa F841, F823

            raise last_exc
        return wrapped
    return wrapper
