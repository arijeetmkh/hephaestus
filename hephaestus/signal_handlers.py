import signal
import logging
from hephaestus import worker

signalhandlerLogger = logging.getLogger('hephaestus.startup')


def signal_handler_sigint(signal, frame):
    signalhandlerLogger.info('Interrupt Signal Received')
    worker.clean_shutdown()

signal.signal(signal.SIGINT, signal_handler_sigint)


def signal_handler_sighup(signal, frame):
    signalhandlerLogger.info('Hang Up Signal Received')
    worker.clean_shutdown()

signal.signal(signal.SIGHUP, signal_handler_sighup)