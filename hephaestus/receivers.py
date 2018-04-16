from __future__ import absolute_import, division, print_function, unicode_literals

import abc


class MessageReceiver(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def process_message(self, message):
        raise NotImplementedError

    def setup_hook(self, conf):
        pass
