import functools
import time
import logging

logger = logging.getLogger(__name__)


def retry_on_failure(max_retries: int = 3, backoff_seconds: int = 1):
    """
    Simple retry decorator with exponential backoff.

    - Retries on any exception up to max_retries.
    - Backoff grows as: backoff_seconds * 2**(attempt-1)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:  # pylint: disable=broad-exception-caught
                    last_exc = exc
                    if attempt == max_retries:
                        logger.error(
                            "Max retries reached for %s. Last error: %s",
                            func.__name__,
                            exc,
                        )
                        raise

                    sleep_for = backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "Error in %s (attempt %s/%s). Retrying in %s seconds...",
                        func.__name__,
                        attempt,
                        max_retries,
                        sleep_for,
                    )
                    time.sleep(sleep_for)
            raise last_exc
        return wrapper
    return decorator
