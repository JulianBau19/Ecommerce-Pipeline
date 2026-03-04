import time

def log_and_time_step(logger):

    def decorator(func):

        def wrapper(*args, **kwargs):

            logger.info(f"Starting step: {func.__name__}")

            start = time.time()

            result = func(*args, **kwargs)

            end = time.time()
            duration = round(end - start, 3)

            logger.info(f"Finished step: {func.__name__} in {duration}s")

            return result

        return wrapper

    return decorator