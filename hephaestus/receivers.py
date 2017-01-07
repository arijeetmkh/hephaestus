import abc


class MessageReceiver(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def process_message(self, message):
        raise NotImplementedError
