import logging
from lbrynet.core.log_support import Logger, TRACE

__version__ = "0.0.3"
version = tuple(__version__.split('.'))

logging.setLoggerClass(Logger)
