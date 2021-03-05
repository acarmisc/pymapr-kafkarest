import json
import os
import urllib
import socket
import logging
import requests
from requests.auth import HTTPBasicAuth
from slugify import slugify

from pymapr_kafkarest.exceptions import MKExistingInstanceException, MKSubscriptionException, MKConsumerException

LOGGING_LEVEL = os.environ.get('KAFKAREST_LOG_LEVEL', 'DEBUG')

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-3.3s]  %(message)s")
logger = logging.getLogger()
logger.setLevel(LOGGING_LEVEL)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


class MaprKafkaBase:

    def __init__(self, base_url, username=None, password=None, headers=None, topics=None, verify=False):
        """
        :param base_url:
        :param username:
        :param password:
        :param headers:
        :param topics:
        :param verify:
        """
        self.base_url = base_url
        self.username = username
        self.password = password

        self.topics = topics if topics is not None else list()

        self.headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
        self.headers.update(**headers)

        self.verify = verify

        if username and password:
            self.auth = HTTPBasicAuth(username, password)
        else:
            self.auth = None


class MaprKProducer(MaprKafkaBase):

    def __init__(self, base_url, username=None, password=None, headers=None):

        super().__init__(base_url, username, password, headers)

    def _produce(self, messages: list, topic=None, partition=None):
        """
        Produces messages to a topic into a partition of a topic.
        If no topic are provided will produce to all topics in the instance.
        :param topic:
        :param message:
        :param partition:
        :return:
        """
        _partition_part = f'/partitions/{partition}' if partition else ''

        _msgs = dict(records=[])
        _msgs['records'] = messages

        headers = self.headers.copy()
        headers['Content-Type'] = 'application/vnd.kafka.json.v2+json'
        headers['Accept'] = 'application/vnd.kafka.v2+json'

        if topic:
            encoded_topic = urllib.parse.quote(topic, safe='')
            logger.debug(f'Posting {len(messages)} to {topic}')
            url = f'{self.base_url}topics/{encoded_topic}{_partition_part}'
        else:
            logger.debug(f'Posting {len(messages)} to all instance topics')
            url = f'{self.base_url}topics/{_partition_part}'

        r = requests.post(url, headers=headers, json=_msgs, auth=self.auth, verify=self.verify)

        assert r.status_code == 200, f'Error creating messages into {topic}: ' \
                                     f'{r.status_code} {r.text}'

    def produce(self, messages, topic=None, partition=0):
        self._produce(messages, topic=topic, partition=partition)


class MaprKlient(MaprKafkaBase):

    def __init__(self, base_url, consumer, username=None, password=None, headers=None,
                 topics=None, instance=None, partition=0, follow_base_uri=False, verify=False):
        """
        :param base_url:
        :param consumer:
        :param username:
        :param password:
        :param headers:
        :param topics:
        :param instance:
        :param partition:
        :param follow_base_uri:
        :param verify:
        """
        super().__init__(base_url, username, password, headers, topics, verify)
        self.consumer = consumer
        self.instance_name = instance or slugify(socket.gethostname())
        self.partition = partition

        self.follow_base_uri = follow_base_uri

        self.consumer_url = f'{self.base_url}consumers/{self.consumer}'
        self.instance_url = self.consumer_url + f'/instances/{self.instance_name}'

    def _topics(self):
        """
        Retrieves a list of topic names.
        :return:
        """
        raise NotImplementedError

    def _topic_info(self, topic: str) -> dict:
        """
        Retrieves metadata about a specific topic.
        :param topic:
        :return:
        """
        raise NotImplementedError

    def _topic_partitions(self, topic: str):
        """
        Retrieves a list of partitions for the topic.
        :param topic:
        :return:
        """
        raise NotImplementedError

    def _topic_partition_metadata(self, topic: str, partition: int):
        """
        Retrieves metadata about a specific partition within a topic.
        :param topic:
        :param partition:
        :return:
        """
        raise NotImplementedError

    def _instance_delete(self):
        """
        Destroys the consumer instance.
        :return:
        """
        logger.debug(f'Deleting instance {self.instance_name} on {self.instance_url}')
        r = requests.delete(self.instance_url, headers=self.headers, auth=self.auth, verify=self.verify)

        assert r.status_code == 204, f'Error deleting instance {self.instance_name}: ' \
                                     f'{r.status_code} {r.text}'

    def _instance(self, auto_offset='earliest', format='json') -> dict:
        """
        Creates a new consumer instance in the consumer group.
        :param group_name:
        :return:
        """
        url = self.consumer_url
        logger.debug(f'Creating instance {self.instance_name} calling {url}...')
        payload = {'name': self.instance_name, 'auto.offset.reset': auto_offset, 'format': format}

        r = requests.post(url, headers=self.headers, json=payload, auth=self.auth, verify=self.verify)
        if r.status_code == 409:
            raise MKExistingInstanceException(f'Instance {self.instance_name} already exists!')

        assert r.status_code == 200, f'Error creating instance {self.instance_name}: [{r.status_code}] {r.text}'

        data = r.json()
        if self.follow_base_uri:
            self.instance_url = data.get('base_uri', None)

        return r.json()

    def _offset_commit(self, offset: int):
        """
        Commits a list of offsets for the consumer. When the post body is empty,
        it commits all the records that have been fetched by the consumer instance.
        curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data '{"offsets": [{"topic":
        "/mystream:first","partition": 0,"offset":5}]}'
        https://node2:8082/consumers/grouptest/instances/user/offsets
        :return:
        """
        raise NotImplementedError

    def _offset(self):
        """
        Gets the last committed offsets for the given partitions (whether the commit
        happened by this process or another).
        :return:
        """
        raise NotImplementedError

    def _subscription(self):
        """
        Subscribes to the given list of topics or a topic pattern to get dynamically assigned partitions.
        If a prior subscription exists, it would be replaced by the latest subscription.
        :return:
        """
        url = self.instance_url + '/subscription'

        payload = dict(topics=[])
        payload['topics'] = self.topics

        logger.info(f'Creating a subscription for {self.instance_name} for topics {self.topics}...')
        r = requests.post(url, headers=self.headers, json=payload, auth=self.auth, verify=self.verify)

        if r.status_code != 204:
            raise MKSubscriptionException(f'({r.status_code}) {r.text}')

        return True

    def _subscription_topics(self):
        """
        Gets the current subscribed list of topics
        curl -X GET -H "Content-Type: application/vnd.kafka.v2+json"
        https://localhost:8082/consumers/grouptest/instances/user/subscription
        :return:
        """
        url = self.instance_url + '/subscription'

        logger.info(f'Checking active subscription for {self.instance_name}')
        r = requests.get(url, headers=self.headers, auth=self.auth, verify=self.verify)

        if r.status_code == 404:
            logger.debug('No active consumer found => no active subscriptions.')
            return {}

        return r.json()

    def _unsubscribe_topics(self):
        """
        Unsubscribes from topics currently subscribed to.
        :return:
        """
        raise NotImplementedError

    def _assignments_commit(self):
        """
        Manually assigns a list of partitions to a consumer.
        curl -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data
        '{"partitions":[{"topic":"first","partition":0}]}'
        https://localhost:8082/consumers/grouptest/instances/user/assignments
        :return:
        """
        raise NotImplementedError

    def _assignments(self):
        """
        Retrieves the list of partitions currently assigned to this consumer.
        :return:
        """
        raise NotImplementedError

    def _position(self, position: int):
        """
        Overrides the fetch offsets that the consumer will use for the next set of records to fetch.
        curl  -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data
        '{"offsets": [{"topic": "/stream:first","partition": 0,"offset":3}]}'
        https://localhost:8082/consumers/grouptest/instances/user/positions
        :return:
        """
        url = self.instance_url + '/positions/'

        payload = {'offsets': []}
        for _t in self.topics:
            payload['offsets'].append({'topic': _t, 'partition': self.partition, 'offset': position})

        logger.info(f'Changing position to {position} for {self.topics}.')
        r = requests.post(url, headers=self.headers, json=payload, auth=self.auth, verify=self.verify)
        assert r.status_code == 204, f'Error positioning to {position}'

    def _seek_beginning(self):
        """
        Seek to the first offset for each of the given partitions.
        curl  -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data
        '{"partitions": [{"topic": "/stream:first","partition": 0}]}'
        https://localhost:8082/consumers/grouptest/instances/user/positions/beginning
        :return:
        """
        url = self.instance_url + '/positions/beginning'

        payload = {'partitions': []}
        for _t in self.topics:
            payload['partitions'].append({'topic': _t, 'partition': self.partition})

        logger.info(f'Changing position to beginning for {self.topics}.')
        r = requests.post(url, headers=self.headers, json=payload, auth=self.auth, verify=self.verify)
        assert r.status_code == 204, f'Error seek to beginning'

    def _seek_end(self):
        """
        Seek to the last offset for each of the given partitions.
        curl  -X POST -H "Content-Type: application/vnd.kafka.v2+json" --data
        '{"partitions": [{"topic": "/stream:first","partition": 0}]}'
        https://localhost:8082/consumers/grouptest/instances/user/positions/end
        :return:
        """
        raise NotImplementedError

    def _records(self, max_bytes=None) -> list:
        """
        Fetches data for the topics or partitions specified using one of the subscribe/assign APIs.
        :return:
        """
        _params = dict()
        url = self.instance_url + '/records'
        logger.info(f'Getting records from {url}')

        if max_bytes:
            _params['max_bytes'] = max_bytes

        _headers = self.headers.copy()
        _headers.pop('Content-Type')
        _headers['Accept'] = 'application/vnd.kafka.json.v2+json'

        r = requests.get(url, headers=_headers, auth=self.auth, params=_params)
        if r.status_code != 200:
            raise MKConsumerException(f'Error consuming messages: [{r.status_code}] {r.text}')

        return r.json()

    def connect(self, clear=False):
        """
        TBD
        :param clear:
        :param clean:
        :return:
        """
        active_subs = self._subscription_topics()
        if active_subs:
            topics = active_subs.get('topics')
            logger.debug(f'Found subscriptions for topics: {topics}')
            if not topics or not set(self.topics).issubset(topics):
                self.subscribe()

        try:
            self._instance()
        except MKExistingInstanceException as e:
            logger.warning(e)
            if clear:
                logger.info('Cleaning previous session...')
                self._instance_delete()
                self.connect(clear=False)

    def subscribe(self):
        """
        TBD
        :return:
        """
        self._subscription()

    def consume(self, seek=None, position=None, max_bytes=None, beginning=None):
        """
        TBD
        :param seek:
        :param position:
        :return:
        """
        if position is not None:
            self._position(position)

        if beginning is not None:
            self._seek_beginning()

        return self._records()
