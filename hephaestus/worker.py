import threading
import queue
import time

from .conf import settings, get_boto_session


messageQueue = queue.Queue(maxsize=settings.MESSAGE_QUEUE_MAX_SIZE)


class SQSWorker(threading.Thread):
    def __init__(self, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.messageQueue = messageQueue

    @staticmethod
    def initialize_queue(queue):
        kwargs = {
            'MaxNumberOfMessages': settings.SQS_MAX_NUMBER_MESSAGES
        }
        if settings.SQS_VISIBILITY_TIMEOUT:
            kwargs['VisibilityTimeout'] = settings.SQS_VISIBILITY_TIMEOUT
        if settings.SQS_WAIT_TIME_SECONDS:
            kwargs['WaitTimeSeconds'] = settings.SQS_WAIT_TIME_SECONDS
        return queue(**kwargs)

    def run(self):
        sqs = get_boto_session().resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=settings.SQS_QUEUE_NAME)
        print(queue)
        while True:
            for message in self.initialize_queue(queue):
                self.messageQueue.put(message)
            time.sleep(1)


class MessageWorker(threading.Thread):
    def __init__(self, **kwargs):
        self.transport = kwargs.pop('transport')
        threading.Thread.__init__(self, **kwargs)
        self.messageQueue = messageQueue

    def run(self):
        while True:
            print("Mesage worker")
            message = self.messageQueue.get()
            self.transport.send(message)


def start_workers(transport=None):
    for i in range(settings.QUEUE_WORKERS):
        SQSWorker(name="SQSWorker-{worker_number}".format(worker_number=str(i))).start()

    for i in range(settings.MESSAGE_PROCESSOR_WORKERS):
        MessageWorker(
            transport=transport,
            name="MessageWorker-{worker_number}".format(worker_number=str(i))
        ).start()
