from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys
import importlib
import logging
from .exceptions import *


transportLogger = logging.getLogger('hephaestus.transport')


class Transport(object):

    _type = None
    klass = None

    def __init__(self, conf):
        self.conf = conf
        self.klass = None

    def setup(self):
        envs = self.conf.get('env', ())
        if not envs:
            return
        assert isinstance(envs, dict), "Environments must be an object holding keys and values"

        transportLogger.info("Settings up environment variables")
        for env, value in envs.items():
            transportLogger.debug('ENV: %s : %s' % (str(env), str(value)))
            os.environ[env] = value

    def load(self):
        raise NotImplementedError

    def send(self, message):
        raise NotImplementedError


class LoggerTransport(Transport):

    _type = "log"
    _logger = None

    def load(self):
        self._logger = logging.getLogger(self.conf['logger'])

    def send(self, message):
        self._logger.info(message.body)


class DjangoTransport(Transport):
    _type = "django"

    @classmethod
    def setup_django(cls, settings_module='', project_path=''):
        try:
            import django
        except ImportError:
            raise TransportRequirementError('Unable to import %s' % cls._type.title())

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
        try:
            klass.process_message(message)
        except Exception as exc:
            transportLogger.exception("Transport received a message receiver exception")
            raise ReceiverError(str(exc))


class FlaskTransport(Transport):
    _type = "flask"

    def __init__(self, *args, **kwargs):
        super(FlaskTransport, self).__init__(*args, **kwargs)
        self._app = None

    @classmethod
    def setup_flask(cls, config_object_path='', project_path=''):
        try:
            import flask
        except ImportError:
            raise TransportRequirementError('Unable to import %s' % cls._type.title())

        sys.path.append(project_path)

        app = flask.Flask(__name__)
        app.config.from_object(config_object_path)
        return app

    def load(self):
        self._app = self.setup_flask(project_path=self.conf['project_path'], config_object_path=self.conf['config_object_path'])
        module = importlib.import_module(self.conf['class_import_path'])
        klass = getattr(module, self.conf['class_name'], None)
        if not klass:
            raise TransportLoadError("Class '%s' not found or not of type 'MessageProcessor'" % self.conf['class_name'])

        self.klass = klass

    def send(self, message):
        klass = self.klass()
        try:
            klass.process_message(message)
        except Exception as exc:
            transportLogger.exception("Transport received a message receiver exception")
            raise ReceiverError(str(exc))


class CustomTransport(Transport):
    """
    A transport which behaves as an adaptor to the actual transport loaded at runtime
    """
    _type = "custom"
