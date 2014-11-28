#!/usr/bin/env python2
import time
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import StreamRestClient
from audi.cdap import MetricsRestClient


class DynamicTruncateTest(unittest.TestCase):
    def setUp(self):
        self.app_id = 'SlowFlowSim'
        self.flow_id = 'SlowFlow'

        jar_file = 'SlowFlowSim-2.5.0-SNAPSHOT.jar'
        audi.upload_app(__file__, jar_file)

        self.client = ClientRestClient()
        self.stream = StreamRestClient()
        self.metrics = MetricsRestClient()

        # get flow details
        self.flow_ids = []
        resp = self.client.flows(self.app_id)
        data = resp.json()
        for el in data:
            self.flow_ids.append(el['id'])

        self.assertEquals(resp.status_code, 200)
        self.assertEquals(self.flow_ids, [self.flow_id])

        # start flow(s)
        for flow_id in self.flow_ids:
            resp = self.client.start_element(self.app_id, 'flows', flow_id)
            self.assertEquals(resp.status_code, 200)

        # check flow status
        time.sleep(5)
        is_running = False
        for i in range(50):
            for flow_id in self.flow_ids:
                resp = self.client.flow_status(self.app_id, flow_id)
                self.assertEquals(resp.status_code, 200)
                if resp.json()['status'] == 'RUNNING':
                    is_running = True
                    break
                else:
                    time.sleep(1)
        self.assertTrue(is_running)

    def tearDown(self):
        # read from stream
        resp = self.stream.read_events('event')

        # stop flow(s)
        for flow_id in self.flow_ids:
            resp = self.client.stop_element(self.app_id, 'flows', self.flow_id)
            self.assertEquals(resp.status_code, 200)

        # check flow status
        time.sleep(5)
        is_running = False
        for i in range(50):
            for flow_id in self.flow_ids:
                resp = self.client.flow_status(self.app_id, self.flow_id)
                self.assertEquals(resp.status_code, 200)
                if resp.json()['status'] == 'STOPPED':
                    is_running = True
                    break
                else:
                    time.sleep(1)
        self.assertTrue(is_running)

        # stop app
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def test_stream(self):
        # send event - 'aaa'
        resp = self.stream.send_event('event', 'aaa')
        self.assertEquals(resp.status_code, 200)

        # check events collected
        time.sleep(5)
        resp = self.metrics.metric_request(
            'system',
            'streams/event',
            'collect.events'
        )
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(resp.json()['data'], 1)

        # check events processed
        # resp = self.metrics.metric_request(
        #     'system',
        #     'apps/SlowFlowSim/flows/SlowFlow/flowlets/reader',
        #     'process.events.processed'
        # )
        # self.assertEquals(resp.status_code, 200)
        # self.assertEquals(resp.json()['data'], 1)

        # send event - 'bbb'
        resp = self.stream.send_event('event', 'bbb')
        self.assertEquals(resp.status_code, 200)

        # send event - 'ccc'
        resp = self.stream.send_event('event', 'ccc')
        self.assertEquals(resp.status_code, 200)

        time.sleep(3)

        # truncate stream
        self.stream.truncate('event')

        # check events collected
        resp = self.metrics.metric_request(
            'system',
            'streams/event',
            'collect.events'
        )
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(resp.json()['data'], 3)

        # check events processed
        # resp = self.metrics.metric_request(
        #     'system',
        #     'apps/SlowFlowSim/flows/SlowFlow/flowlets/reader',
        #     'process.events.processed'
        # )
        # self.assertEquals(resp.status_code, 200)
        # self.assertEquals(resp.json()['data'], 1)
