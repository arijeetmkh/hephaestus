import logging
import boto3

from .transports import DjangoTransport, CustomTransport
from .exceptions import *

confLogger = logging.getLogger('hephaestus.conf')


class Settings(object):
    pass

settings = Settings()

transports = {
    "django": DjangoTransport,
    "custom": CustomTransport
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
    return boto3.session.Session(
        aws_access_key_id=settings.AWS_KEY,
        aws_secret_access_key=settings.AWS_SECRET,
        region_name=settings.AWS_REGION
    )
