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

    @classmethod
    def setup_django(cls, settings_module='', project_path=''):
        try:
            import django
        except ImportError:
            raise TransportRequirementError('Unable to import Django')

        os.environ['DJANGO_SETTINGS_MODULE'] = settings_module
        sys.path.append(project_path)
        try:
            django.setup()
        except django.core.exceptions.ImproperlyConfigured:
            raise TransportLoadError('Django setup failed')

    def load(self):
        self.setup_django(settings_module=self.conf['settings_module'], project_path=self.conf['project_path'])
        module = importlib.import_module(self.conf['class_import_path'])
        klass = getattr(module, self.conf['class_name'], None)
        if not klass:
            raise TransportLoadError("Class '%s' not found or not of type 'MessageProcessor'" % self.conf['class_name'])

        self.klass = klass

    def send(self, message):
        klass = self.klass()
        klass.process_message(message)


class CustomTransport(Transport):
    """
    A transport which behaves as an adaptor to the actual transport loaded at runtime
    """
    _type = "custom"
