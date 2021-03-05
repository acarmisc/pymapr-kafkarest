from setuptools import setup, find_packages

VERSION = '0.1.0'
DESCRIPTION = 'Python MAPR Kafka REST wrapper'
LONG_DESCRIPTION = 'Lazy way to interact with MAPR Kafka REST proxy from python.'

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="pymapr-kafkarest",
    version=VERSION,
    author="Andrea Carmisciano",
    author_email="andrea.carmisciano@gmail.com",
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=['requests', 'urllib3'],
    keywords=['MAPR', 'Kafka', 'REST', 'data'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)