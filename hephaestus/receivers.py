from __future__ import absolute_import, division, print_function, unicode_literals
from six import with_metaclass

import abc


class MessageReceiver(with_metaclass(abc.ABCMeta, object)):

    @abc.abstractmethod
    def process_message(self, message):
        raise NotImplementedError

    def setup_hook(self, conf):
        pass
