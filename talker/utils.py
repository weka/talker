import time
import random
import logging
import redis.exceptions
from functools import wraps

# Assuming talker.errors and talker.config will be available for import
# We will define TalkerRedisOpFailedError in talker.errors.py
# We will use _logger from talker.config
from talker.errors import TalkerRedisOpFailedError
from talker.config import _logger

# Configuration for retry logic
REDIS_RETRY_MAX_ATTEMPTS = 50
REDIS_RETRY_INITIAL_BACKOFF_SEC = 1
REDIS_RETRY_MAX_BACKOFF_SEC = 30
REDIS_RETRY_JITTER_FACTOR = 0.5  # e.g., 0.5 means +/- 50% of current_backoff

def retry_redis_op(func):
    """
    Decorator to retry a Redis operation with exponential backoff and jitter.
    Catches redis.exceptions.ConnectionError and redis.exceptions.TimeoutError.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        attempt = 0
        current_backoff = REDIS_RETRY_INITIAL_BACKOFF_SEC
        last_exception = None
        
        log_extra = kwargs.get('log_context_extra', {})

        while attempt < REDIS_RETRY_MAX_ATTEMPTS:
            try:
                return func(*args, **kwargs)
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
                attempt += 1
                last_exception = e
                
                current_log_context = {**log_extra}
                current_log_context.update({
                    'redis_op_func': func.__name__,
                    'attempt': attempt,
                    'max_attempts': REDIS_RETRY_MAX_ATTEMPTS,
                })

                if attempt >= REDIS_RETRY_MAX_ATTEMPTS:
                    _logger.error(
                        "Redis operation '%s' failed after %d attempts: %s. No more retries.",
                        func.__name__, REDIS_RETRY_MAX_ATTEMPTS, e,
                        extra=current_log_context
                    )
                    raise TalkerRedisOpFailedError(
                        f"Redis operation '{func.__name__}' failed after {REDIS_RETRY_MAX_ATTEMPTS} attempts",
                        original_exception=e,
                        attempts=attempt,
                        func_name=func.__name__,
                        args_summary=str(args)[:200],
                        kwargs_summary=str(kwargs)[:200]
                    ) from e

                jitter_amount = random.uniform(-REDIS_RETRY_JITTER_FACTOR, REDIS_RETRY_JITTER_FACTOR) * current_backoff
                sleep_duration = max(0.1, current_backoff + jitter_amount)  # Ensure minimum sleep, prevent negative

                _logger.warning(
                    "Redis operation '%s' failed (attempt %d/%d): %s. Retrying in %.2f seconds...",
                    func.__name__, attempt, REDIS_RETRY_MAX_ATTEMPTS, e, sleep_duration,
                    extra=current_log_context
                )
                time.sleep(sleep_duration)

                # Exponential backoff
                current_backoff = min(REDIS_RETRY_MAX_BACKOFF_SEC, current_backoff * 2)
        
        if last_exception: # pragma: no cover
             _logger.error(
                "Redis operation '%s' failed (loop exited unexpectedly or no retries configured): %s",
                func.__name__, last_exception,
                extra=log_extra
             )
             raise TalkerRedisOpFailedError(
                f"Redis operation '{func.__name__}' failed (no retries configured or loop exited unexpectedly)",
                original_exception=last_exception,
                attempts=attempt, 
                func_name=func.__name__,
                args_summary=str(args)[:200],
                kwargs_summary=str(kwargs)[:200]
            ) from last_exception
        return None # pragma: no cover
    return wrapper
