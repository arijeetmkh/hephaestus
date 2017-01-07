import boto3

from .transports import DjangoTransport


class Settings(object):
    pass

settings = Settings()

processor_setup = {
    "django": DjangoTransport,
}


def get_boto_session():
    return boto3.session.Session(
        aws_access_key_id=settings.AWS_KEY,
        aws_secret_access_key=settings.AWS_SECRET,
        region_name=settings.AWS_REGION
    )
