import signal
import logging
from hephaestus import worker


signal_name_id_mapping = dict((k, v) for v, k in reversed(sorted(signal.__dict__.items()))
     if v.startswith('SIG') and not v.startswith('SIG_'))


def signal_handler(sig, frame):
    signalhandlerLogger = logging.getLogger('hephaestus.startup')
    signalhandlerLogger.info('%s Signal Received' % signal_name_id_mapping[sig])
    worker.clean_shutdown()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
