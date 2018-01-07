from __future__ import absolute_import, division, print_function, unicode_literals
import logging
import boto3
import json

from pkg_resources import resource_filename
from .transports import DjangoTransport, LoggerTransport, FlaskTransport, PythonTransport, LambdaTransport
from .exceptions import *

confLogger = logging.getLogger('hephaestus.conf')
startupLogger = logging.getLogger('hephaestus.startup')


class Settings(object):

    def update_settings(self, settings):
        for k, v in settings.items():
            setattr(self, k, v)


settings = Settings()

transports = {
    "django": DjangoTransport,
    "flask": FlaskTransport,
    "python": PythonTransport,
    "lambda": LambdaTransport,
    "log": LoggerTransport,
}


def get_log_config(level):
    return {
        'version': 1,
        'formatters': {
            'basicFormatter': {
                'format': '[%(asctime)s %(levelname)s %(threadName)s] %(name)s: %(message)s'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'basicFormatter',
                'stream': 'ext://sys.stdout'
            }
        },
        'loggers': {
            'hephaestus': {
                'level': logging.getLevelName(level),
                'propagate': False,
                'handlers': ['console']
            }
        }
    }


def load_transport(transport_type):
    try:
        t_type = transports[transport_type]
        confLogger.info("Loaded transport %s" % str(t_type))
        return t_type
    except KeyError:
        confLogger.critical("Transport '%s' not found" % str(transport_type))
        raise TransportNotFound("Transport '%s' not found" % str(transport_type))


def get_boto_session():
    confLogger.debug('Retrieving Boto session with values - aws_access_key_id=%s, aws_secret_access_key=%s, region_name=%s' % (settings.AWS_KEY, settings.AWS_SECRET, settings.AWS_REGION))
    if not settings.AWS_REGION:
        raise ConfigError('AWS_REGION not defined')
    return boto3.session.Session(
        aws_access_key_id=settings.AWS_KEY,
        aws_secret_access_key=settings.AWS_SECRET,
        region_name=settings.AWS_REGION
    )


def set_config(config, args):

    def get_config(key, config_section_key, config_get_type=None, default=None):
        arg = getattr(args, key, None)
        if arg:
            if config_get_type:
                return config_get_type(arg)
            else:
                return arg

        config_section = config[config_section_key]
        value = config_section.get(key)
        if value:
            if not config_get_type:
                return value
            elif config_get_type == int:
                return config_section.getint(key)

        return default

    _settings = {'AWS_KEY': get_config('aws_key', 'AWS_CREDENTIALS'),
                 'AWS_SECRET': get_config('aws_secret', 'AWS_CREDENTIALS'),
                 'AWS_REGION': get_config('aws_region', 'AWS_CREDENTIALS'),
                 'SQS_QUEUE_NAME': get_config('queue_name', 'SQS_SETTINGS'),
                 'SQS_VISIBILITY_TIMEOUT': get_config('visibility_timeout', 'SQS_SETTINGS', int),
                 'SQS_MAX_NUMBER_MESSAGES': get_config('max_number_of_messages', 'SQS_SETTINGS', int),
                 'SQS_WAIT_TIME_SECONDS': get_config('wait_time_seconds', 'SQS_SETTINGS', int),
                 'SQS_WAIT_BETWEEN_REQUESTS': get_config('wait_between_requests', 'SQS_SETTINGS', int),
                 'SQS_MESSAGE_DELETE_POLICY': get_config('message_delete_policy', 'SQS_SETTINGS'),
                 'QUEUE_WORKERS': get_config('queue_workers', 'WORKER_SETTINGS', int),
                 'MESSAGE_PROCESSOR_WORKERS': get_config('message_processor_workers', 'WORKER_SETTINGS', int),
                 'MESSAGE_QUEUE_MAX_SIZE': get_config('message_queue_max_size', 'WORKER_SETTINGS', int),
                 'RECONNECT_WAIT_TIME': get_config("reconnect_wait_time", "GENERAL", int, default=10),
                 'MESSAGE_QUEUE_WAIT_TIMEOUT': get_config('message_queue_wait_timeout', "WORKER_SETTINGS", int, default=10)
                 }

    transport_conf = get_config('message_transport_conf', 'GENERAL')
    if not transport_conf:
        transport_conf = resource_filename("hephaestus", "message_transport_conf.json")
    _settings['MESSAGE_TRANSPORT_CONF'] = transport_conf

    settings.update_settings(_settings)
    startupLogger.debug('Config set as - ' + str(settings.__dict__))


def set_transports():
    startupLogger.info('Transport config file - %s' % str(settings.MESSAGE_TRANSPORT_CONF))
    try:
        message_transport_conf = json.load(open(settings.MESSAGE_TRANSPORT_CONF))
    except FileNotFoundError:
        raise TransportFileNotFound("Transport Configuration File Not Found at '%s'" % str(settings.MESSAGE_TRANSPORT_CONF))

    startupLogger.info('Loading transport type - %s' % message_transport_conf['type'])
    Transport = load_transport(message_transport_conf['type'])
    transport = Transport(conf=message_transport_conf)
    transport.setup()
    transport.load()
    return transport


def verify_settings():
    #ToDo: Use cerberus to verify setting
    assert settings.SQS_MESSAGE_DELETE_POLICY in ('immediate', 'after_message_processing', 'after_successful_message_processing', "no_delete"), "Unknown message delete policy"
