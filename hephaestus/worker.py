import logging
import threading
import queue
import time

from .conf import settings, get_boto_session
from .exceptions import *


workerLogger = logging.getLogger('hephaestus.worker')
messageReceiverLogger = logging.getLogger('hephaestus.message_receiver')


class SQSWorker(threading.Thread):
    def __init__(self, **kwargs):
        self.messageQueue = kwargs.pop('messageQueue')
        threading.Thread.__init__(self, **kwargs)
        workerLogger.info('Queue worker started - %s' % str(self.name))

    @staticmethod
    def init_receive_params():
        params = {
            'MaxNumberOfMessages': settings.SQS_MAX_NUMBER_MESSAGES
        }
        if settings.SQS_VISIBILITY_TIMEOUT:
            params['VisibilityTimeout'] = settings.SQS_VISIBILITY_TIMEOUT
        if settings.SQS_WAIT_TIME_SECONDS:
            params['WaitTimeSeconds'] = settings.SQS_WAIT_TIME_SECONDS
        return params

    def run(self):
        sqs = get_boto_session().resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=settings.SQS_QUEUE_NAME)
        print(queue)
        receive_params = self.init_receive_params()
        while True:
            self.messageQueue.put('gagaga')
            for message in queue.receive_messages(**receive_params):
                self.messageQueue.put(message)
            time.sleep(settings.SQS_WAIT_BETWEEN_REQUESTS)


class MessageWorker(threading.Thread):
    def __init__(self, **kwargs):
        self.messageQueue = kwargs.pop('messageQueue')
        self.transport = kwargs.pop('transport')
        threading.Thread.__init__(self, **kwargs)
        workerLogger.info('Message worker started - %s' % str(self.name))

    def run(self):
        while True:
            message = self.messageQueue.get()
            try:
                self.transport.send(message)
            except ReceiverError:
                messageReceiverLogger.exception("Message receiver errored out with an exception")


def start_workers(transport=None):
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
