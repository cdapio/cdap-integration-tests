#!/usr/bin/env python2
import time
import json
import random
import unittest

from audi.cdap import StreamRestClient
from audi.cdap import ClientRestClient
from audi.cdap import DatasetRestClient


class StreamAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.stream_id = 'testStream'
        self.stream = StreamRestClient()
        self.client = ClientRestClient()
        self.dataset = DatasetRestClient()

    def tearDown(self):
        self.client.unrecoverable_reset()
        time.sleep(5)

    def preload_stream_with_events(self, stream_id):
        # create streams
        req = self.stream.create(stream_id)
        if req.status_code != 200:
            err = 'Failed to create stream {0}!'.format(stream_id)
            raise RuntimeError(err)

        # send streams
        name = ['chris', 'sree', 'poorna']
        product = ['iphone', 'iwatch', 'macbook']

        for i in range(100):
            data = '{0} bought {1} {2} for ${3}'.format(
                random.sample(name, 1)[0],
                random.randint(0, 10),
                random.sample(product, 1)[0],
                random.randint(0, 1000000)
            )
            self.stream.send_event(stream_id, data)

    def test_create_stream(self):
        # before create
        req = self.stream.list_streams()
        before = len(json.loads(req.content))

        # create stream and check status code
        req = self.stream.create(self.stream_id)
        self.assertEquals(req.status_code, 200)

        # after create
        req = self.stream.list_streams()
        after = len(json.loads(req.content))
        self.assertNotEquals(before, after)

    def test_send_event_to_stream(self):
        # create stream
        self.stream.create(self.stream_id)

        # test send event to stream and check status code
        data = 'chris bought 1 life for $1000000'
        req = self.stream.send_event(self.stream_id, data)
        self.assertEquals(req.status_code, 200)

    def test_read_events_from_stream(self):
        # preload stream with events
        self.preload_stream_with_events(self.stream_id)

        # test read events from stream without parameters
        req = self.stream.read_events(self.stream_id)
        self.assertEquals(req.status_code, 200)
        self.assertNotEquals(len(req.content), 0)

        # test non-existent stream
        stream_id = 'nonExistentStream'
        req = self.stream.read_events(stream_id)
        self.assertEquals(req.status_code, 404)

    def test_truncate_stream(self):
        # preload stream with events
        self.preload_stream_with_events(self.stream_id)

        # test truncate stream simple
        # truncate
        req = self.stream.truncate(self.stream_id)
        self.assertEquals(req.status_code, 200)

        # double check if truncate was successful
        req = self.stream.read_events(self.stream_id)
        self.assertEquals(req.status_code, 204)
        self.assertEquals(len(req.content), 0)

    def test_ttl_for_stream(self):
        # create stream
        self.stream.create(self.stream_id)

        # check for streams before ttl is set
        req = self.stream.get_info(self.stream_id)
        ttl_before = json.loads(req.content)['ttl']

        # set ttl for stream
        req = self.stream.set_ttl(self.stream_id, ttl_before - 1)
        self.assertEquals(req.status_code, 200)
        time.sleep(1)

        # check for streams before ttl is set
        req = self.stream.get_info(self.stream_id)
        ttl_after = json.loads(req.content)['ttl']
        self.assertNotEquals(ttl_before, ttl_after)
