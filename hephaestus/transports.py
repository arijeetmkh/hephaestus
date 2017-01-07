from .exceptions import *

import os
import sys
import importlib


class Transport(object):

    _type = None
    klass = None

    def __init__(self, conf):
        self.conf = conf
        self.klass = None

    def load(self):
        raise NotImplementedError

    def send(self, message):
        print(message)


class DjangoTransport(Transport):
    _type = "django"

    def load(self):
        try:
            import django
        except ImportError:
            raise TransportRequirementError('Unable to import Django')

        os.environ['DJANGO_SETTINGS_MODULE'] = self.conf['settings_module']
        sys.path.append(self.conf['project_path'])
        django.setup()
        module = importlib.import_module(self.conf['class_import_path'])
        klass = getattr(module, self.conf['class_name'], None)
        if not klass:
            raise TransportLoadError("Class '%s' not found or not of type 'MessageProcessor'" % self.conf['class_name'])

        self.klass = klass

    def send(self, message):
        klass = self.klass()
        klass.process_message(message)
