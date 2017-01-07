from distutils.core import setup


setup(
    name='hephaestus',
    version='0.1.0',
    description='Queue Consumer',
    author='Arijeet Mukherjee',
    author_email='arijeet.mkh@gmail.com',
    url="https://github.com/arijeetmkh/hephaestus",
    packages=['hephaestus'],
    requires=['boto3'],
    scripts=['hephaestus/startup.py']
)