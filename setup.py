from distutils.core import setup


setup(
    name='Hephaestus',
    version='0.1.0',
    description='Queue Consumer',
    author='Arijeet Mukherjee',
    author_email='arijeet.mkh@gmail.com',
    packages=['hephaestus'],
    requires=['boto3'],
    scripts=['hephaestus/startup.py']
)