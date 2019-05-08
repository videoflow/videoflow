import logging

_default_logger = logging.getLogger('videoflow')

def set_verbosity(verbosity : str = "WARNING"):
    verbosity_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }

    if verbosity not in verbosity_map:
        raise ValueError("Logging verbosity is not one of 'DEBUG', 'INFO', 'WARNING', 'ERROR' or 'CRITICAL")
    
    _default_logger.setLevel(verbosity_map[verbosity])
    
def debug(message, *args, **kwargs):
    _default_logger.debug(message, *args, **kwargs)

def error(message, *args, **kwargs):
    _default_logger.error(message, *args, **kwargs)

def warning(message, *args, **kwargs):
    _default_logger.warning(message, *args, **kwargs)

def info(message, *args, **kwargs):
    _default_logger.info(message, *args, **kwargs)

def critical(message, *args, **kwargs):
    _default_logger.critical(message, *args, **kwargs)