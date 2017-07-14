from __future__ import absolute_import, division, print_function, unicode_literals
from botocore.vendored.requests import ConnectionError, Timeout
from botocore.exceptions import EndpointConnectionError
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
_shutdownEvent = threading.Event()


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
        while not _shutdownEvent.is_set():
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
            else:
                workerLogger.debug("Waiting between SQS requests for %d seconds" % settings.SQS_WAIT_BETWEEN_REQUESTS)
                time.sleep(settings.SQS_WAIT_BETWEEN_REQUESTS)


class MessageWorker(threading.Thread):
    def __init__(self, **kwargs):
        self.messageQueue = kwargs.pop('messageQueue')
        self.transport = kwargs.pop('transport')
        threading.Thread.__init__(self, **kwargs)
        workerLogger.info('Message worker started - %s' % str(self.name))

    def run(self):
        while not _shutdownEvent.is_set():
            try:
                message = self.messageQueue.get(True, 10)
            except queue.Empty:
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


def start_workers(transport=None):
    _shutdownEvent.clear()
    workerLogger.debug('Initializing internal message queue with size - %d' % settings.MESSAGE_QUEUE_MAX_SIZE)
    messageQueue = queue.Queue(maxsize=settings.MESSAGE_QUEUE_MAX_SIZE)

    workerLogger.info('Spawning %d queue workers' % settings.QUEUE_WORKERS)
    for i in range(settings.QUEUE_WORKERS):
        SQSWorker(
            messageQueue=messageQueue,
            name="SQSWorker-{worker_number}".format(worker_number=str(i))
        ).start()

    workerLogger.info('Spawning %d message processor workers' % settings.MESSAGE_PROCESSOR_WORKERS)
    for i in range(settings.MESSAGE_PROCESSOR_WORKERS):
        MessageWorker(
            messageQueue=messageQueue,
            transport=transport,
            name="MessageWorker-{worker_number}".format(worker_number=str(i))
        ).start()


def clean_shutdown():
    workerLogger.info('Received Interrupt. Starting clean shutdown')
    _shutdownEvent.set()