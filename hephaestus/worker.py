from __future__ import absolute_import, division, print_function, unicode_literals
from botocore.vendored.requests import ConnectionError, Timeout
import logging
import threading
import time

from .conf import settings, get_boto_session
from .exceptions import *

try:
    import queue
except ImportError:
    import Queue as queue


workerLogger = logging.getLogger('hephaestus.worker')
messageReceiverLogger = logging.getLogger('hephaestus.message_receiver')
_SQSWorkerShutdownEvent = threading.Event()
_MessageWorkerShutdownEvent = threading.Event()


class SQSWorker(threading.Thread):
    def __init__(self, **kwargs):
        self.messageQueue = kwargs.pop('messageQueue')
        threading.Thread.__init__(self, **kwargs)
        workerLogger.info('Queue worker started - %s' % str(self.name))

    @staticmethod
    def init_receive_params():
        params = {
            'MaxNumberOfMessages': settings.SQS_MAX_NUMBER_MESSAGES,
            'AttributeNames': ['All']
        }
        if settings.SQS_VISIBILITY_TIMEOUT:
            params['VisibilityTimeout'] = settings.SQS_VISIBILITY_TIMEOUT
        if settings.SQS_WAIT_TIME_SECONDS:
            params['WaitTimeSeconds'] = settings.SQS_WAIT_TIME_SECONDS
        return params

    def run(self):
        sqs = get_boto_session().resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=settings.SQS_QUEUE_NAME)
        workerLogger.info("SQS Queue- %s" % str(queue))
        receive_params = self.init_receive_params()
        while not _SQSWorkerShutdownEvent.is_set():
            workerLogger.debug("Waiting between SQS requests for %d seconds" % settings.SQS_WAIT_BETWEEN_REQUESTS)
            time.sleep(settings.SQS_WAIT_BETWEEN_REQUESTS)
            workerLogger.debug("Connecting to SQS to receive messages with params %s" % str(receive_params))
            try:
                for message in queue.receive_messages(**receive_params):
                    workerLogger.info(message.body)
                    self.messageQueue.put(message)
                    if settings.SQS_MESSAGE_DELETE_POLICY == "immediate":
                        message.delete()
                        workerLogger.debug("Message deleted according to policy '%s'" % str(settings.SQS_MESSAGE_DELETE_POLICY))
            except (ConnectionError, Timeout) as exc:
                workerLogger.exception("Connection to queue failed. Retrying in %d seconds. Original exception: " % settings.RECONNECT_WAIT_TIME)
                time.sleep(settings.RECONNECT_WAIT_TIME)

        if _SQSWorkerShutdownEvent.is_set():
            workerLogger.info('Exiting...')


class MessageWorker(threading.Thread):
    def __init__(self, **kwargs):
        self.messageQueue = kwargs.pop('messageQueue')
        self.transport = kwargs.pop('transport')
        threading.Thread.__init__(self, **kwargs)
        workerLogger.info('Message worker started - %s' % str(self.name))

    def run(self):
        while not _MessageWorkerShutdownEvent.is_set():
            try:
                message = self.messageQueue.get(True, settings.MESSAGE_QUEUE_WAIT_TIMEOUT)
            except queue.Empty:
                workerLogger.debug("Message local queue wait timeout passed. Trying again")
                continue
            failure = False
            try:
                self.transport.send(message)
            except ReceiverError:
                failure = True
                messageReceiverLogger.warning("Detected error in transport")

            if settings.SQS_MESSAGE_DELETE_POLICY != "immediate":
                if settings.SQS_MESSAGE_DELETE_POLICY == "after_message_processing":
                    message.delete()
                    workerLogger.debug("Message deleted according to policy '%s'" % str(settings.SQS_MESSAGE_DELETE_POLICY))
                elif not failure and settings.SQS_MESSAGE_DELETE_POLICY == "after_successful_message_processing":
                    message.delete()
                    workerLogger.debug("Message deleted according to policy '%s'" % str(settings.SQS_MESSAGE_DELETE_POLICY))

        if _MessageWorkerShutdownEvent.is_set():
            workerLogger.info('Exiting...')


_SQSWorkers, _MessageWorkers = [], []


def start_workers(transport=None):
    _SQSWorkerShutdownEvent.clear()
    _MessageWorkerShutdownEvent.clear()
    workerLogger.debug('Initializing internal message queue with size - %d' % settings.MESSAGE_QUEUE_MAX_SIZE)
    messageQueue = queue.Queue(maxsize=settings.MESSAGE_QUEUE_MAX_SIZE)

    workerLogger.info('Spawning %d queue workers' % settings.QUEUE_WORKERS)
    for i in range(settings.QUEUE_WORKERS):
        t = SQSWorker(
            messageQueue=messageQueue,
            name="SQSWorker-{worker_number}".format(worker_number=str(i))
        )
        _SQSWorkers.append(t)
        t.start()

    workerLogger.info('Spawning %d message processor workers' % settings.MESSAGE_PROCESSOR_WORKERS)
    for i in range(settings.MESSAGE_PROCESSOR_WORKERS):
        t = MessageWorker(
            messageQueue=messageQueue,
            transport=transport,
            name="MessageWorker-{worker_number}".format(worker_number=str(i))
        )
        _MessageWorkers.append(t)
        t.start()


def clean_shutdown():
    if _SQSWorkerShutdownEvent.is_set() or _MessageWorkerShutdownEvent.is_set():
        workerLogger.warning('Clean shutdown already initiated. ** Cold shutdown not yet implemented **')
    else:
        workerLogger.info('Received Interrupt. Starting clean shutdown')

    workerLogger.debug("Sending shutdown event to SQS Workers")
    _SQSWorkerShutdownEvent.set()
    [_.join() for _ in _SQSWorkers]

    workerLogger.info("All SQS Workers exited")

    while not _MessageWorkers[0].messageQueue.empty():
        workerLogger.debug("Local message queue not empty. Waiting...")
        time.sleep(0.5)

    if _MessageWorkers:
        workerLogger.debug("Sending shutdown event to Message Workers")
        _MessageWorkerShutdownEvent.set()
