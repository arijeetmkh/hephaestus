from __future__ import absolute_import, division, print_function, unicode_literals

import abc


class MessageReceiver(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def process_message(self, message):
        raise NotImplementedError
