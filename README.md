# pymapr-kafkarest


## Introduction

The aim of the project is to create a super-simple wrapper for MAPR Kafka REST proxy.
We're trying to allow users to interact with MAPR Kafka in an elementary way and writing 
less code as possible.

The original documentation of the REST calls can be found [here](https://docs.datafabric.hpe.com/61/Kafka/REST-proxy.html).

## Installation

**Soon** available via pip:

```shell
pip install pymapr-kafkarest
```

## Runtime configurations

- `KAFKAREST_LOG_LEVEL`: allows to set the log level... default to `DEBUG`

TBC

## Usage

Import the lib

```python
from pymapr_kafkarest import MaprKlient
```

Define basic attributes

```python
base_url = 'http://my-endopoint:8082'
user_group = 'foo'
topics = ['/streams/foo:bar']
```

and instantiate the client

```python
mk = MaprKlient(base_url, user_group, topics=topics)
```

Connect, subscribe and consume messages as follows:

```python
base_url = 'http://my-endopoint:8082'

if __name__ == '__main__':
    mk = MaprKlient(base_url, user_group, headers=headers, topics=topics)
    mk.connect(clear=True)
    mk.subscribe()
    
    messages = mk.consume()
    
    print(messages)
```

Connect and produce messages:

```python
from pymapr_kafkarest import MaprKlient
from pymapr_kafkarest.kafkarest import MaprKProducer

base_url = 'https://localhost:8082'
headers = {}
user_group = 'me'

if __name__ == '__main__':

    # producing
    mp = MaprKProducer(base_url, user_group, headers=headers)

    msgs = [dict(key='0099', value=dict(color='red', shape='square'))]

    mp.produce(messages=msgs, topic='/streams/foo')

```
## TODO

- [ ] a lot of methods are not yet implemented
- [ ] full read the docs
- [ ] chain `connect`, `subscribe` and `consume` in a new method named `stream` 