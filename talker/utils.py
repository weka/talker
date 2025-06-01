import time
import logging

# Using a default logger if none is provided, or allowing user to pass one.
# For this specific use case, the logger will be passed from the reactor.
_internal_logger = logging.getLogger(__name__)


def retry_on_exception(
        func,
        exceptions_to_catch,
        max_attempts=3,
        delay_seconds=1,
        backoff_factor=2,
        logger=None
):
    """
    Retries a function call if specific exceptions are raised, with exponential backoff.

    :param func: The function to execute.
    :param exceptions_to_catch: A tuple of exception classes to catch and retry on.
    :param max_attempts: Maximum number of attempts.
    :param delay_seconds: Initial delay between retries in seconds.
    :param backoff_factor: Factor by which the delay increases after each attempt.
    :param logger: Optional logger instance for logging retry attempts.
    :return: The result of the function if successful.
    :raises: The last caught exception if all attempts fail.
    """
    effective_logger = logger if logger else _internal_logger
    current_attempts = 0
    current_delay = float(delay_seconds)

    while True:
        current_attempts += 1
        try:
            return func()
        except exceptions_to_catch as e:
            func_name = getattr(func, '__name__', 'callable')
            if current_attempts >= max_attempts:
                effective_logger.error(
                    f"All {max_attempts} attempts failed for {func_name}. Last error: {e}"
                )
                raise
            effective_logger.warning(
                f"Attempt {current_attempts}/{max_attempts} failed for {func_name} with error: {e}. "
                f"Retrying in {current_delay:.2f}s..."
            )
            time.sleep(current_delay)
            current_delay *= backoff_factor
