import logging
from twisted.python import log as twisted_log

from lbrynet.core.log_support import Logger
logging.setLoggerClass(Logger)


observer = twisted_log.PythonLoggingObserver(__name__)
observer.start()

log = logging.getLogger(__name__)

DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s:%(lineno)d: %(message)s"
DEFAULT_FORMATTER = logging.Formatter(DEFAULT_FORMAT)

h = logging.StreamHandler()
h.setFormatter(DEFAULT_FORMATTER)
log.addHandler(h)
log.setLevel(logging.INFO)

__version__ = "0.0.6"
version = tuple(__version__.split('.'))
