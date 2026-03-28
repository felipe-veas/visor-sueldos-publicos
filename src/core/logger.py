import logging
import sys
import time
from functools import wraps


class StructuredFormatter(logging.Formatter):
    def format(self, record):
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")

        level = record.levelname[:3].upper()
        if level == "WAR":
            level = "WRN"

        msg = record.getMessage()

        # Extract extra kwargs
        extras = []
        for key, value in record.__dict__.items():
            if key not in [
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
                "taskName",
                "color_message",
            ]:
                # Replace newlines in values to keep log on a single line
                val_str = str(value).replace("\n", " ")
                extras.append(f"{key}={val_str}")

        extras_str = " ".join(extras)
        if extras_str:
            return f"visor-app      | {timestamp} {level} {msg} {extras_str}"
        return f"visor-app      | {timestamp} {level} {msg}"


def get_logger(name="visor"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(StructuredFormatter())
        logger.addHandler(handler)
        # Prevent double logging if the root logger also has handlers
        logger.propagate = False
    return logger


logger = get_logger()


def log_action(action_name):
    """Decorator to log function execution time and status."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start

                # Check if it's a pandas dataframe to log row count
                rows = (
                    getattr(result, "shape", [None])[0]
                    if hasattr(result, "shape")
                    else None
                )

                extra_data = {"duration": round(duration, 5), "status": "success"}
                if rows is not None:
                    extra_data["rows"] = rows

                logger.info(f"{action_name} completed", extra=extra_data)
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(
                    f"{action_name} failed",
                    extra={
                        "duration": round(duration, 5),
                        "error": str(e),
                        "status": "error",
                    },
                )
                raise

        return wrapper

    return decorator
