import threading
import queue

from .conf import settings, get_boto_session


messageQueue = queue.Queue(maxsize=10)


class SQSWorker(threading.Thread):
    def __init__(self, **kwargs):
        threading.Thread.__init__(self, **kwargs)
        self.messageQueue = messageQueue

    def run(self):
        sqs = get_boto_session().resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=settings.AWS_QUEUE_NAME)
        print(queue)
        self.messageQueue.put("MEH %s" % str(self.name))


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
