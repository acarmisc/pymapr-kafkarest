from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'Python MAPR Kafka REST wrapper'
LONG_DESCRIPTION = 'Lazy way to interact with MAPR Kafka REST proxy from python.'

setup(
    name="pymapr-kafkarest",
    version=VERSION,
    author="Andrea Carmisciano",
    author_email="andrea.carmisciano@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
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