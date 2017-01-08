import boto3

from .transports import DjangoTransport, CustomTransport
from .exceptions import *


class Settings(object):
    pass

settings = Settings()

transports = {
    "django": DjangoTransport,
    "custom": CustomTransport
}


def load_transport(transport_type):
    try:
        return transports[transport_type]
    except KeyError:
        raise TransportNotFound("Transport '%s' not found" % str(transport_type))


def get_boto_session():
    return boto3.session.Session(
        aws_access_key_id=settings.AWS_KEY,
        aws_secret_access_key=settings.AWS_SECRET,
        region_name=settings.AWS_REGION
    )
