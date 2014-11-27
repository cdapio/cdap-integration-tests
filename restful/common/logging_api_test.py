#!/usr/bin/env python2
import time
import random
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import StreamRestClient
from audi.cdap import LoggingRestClient
from audi.cdap import DatasetRestClient


class LoggingAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'PurchaseHistory'
        self.flow_id = 'PurchaseFlow'
        self.stream_id = 'purchaseStream'
        self.procedure_id = 'PurchaseProcedure'

        self.client = ClientRestClient()
        self.stream = StreamRestClient()
        self.logging = LoggingRestClient()
        self.dataset = DatasetRestClient()

        # deploy app
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['Purchase'])

    @classmethod
    def tearDownClass(self):
        # stop app
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def setUp(self):
        time.sleep(2)
        self.start = int(time.time())

        # start flow
        resp = self.client.start_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(resp.status_code, 200)
        time.sleep(2)

        # send evenets to stream
        name = ['chris', 'sree', 'poorna']
        product = ['iphone', 'iwatch', 'macbook']

        for i in range(100):
            data = '{0} bought {1} {2} for ${3}'.format(
                random.sample(name, 1)[0],
                random.randint(0, 10),
                random.sample(product, 1)[0],
                random.randint(0, 1000000)
            )
            self.stream.send_event(self.stream_id, data)
        time.sleep(3)

    def tearDown(self):
        # stop flow
        resp = self.client.stop_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(resp.status_code, 200)
        time.sleep(2)

    def test_download_logs(self):
        # get logs
        stop = self.start - (self.start - int(time.time()))
        resp = self.logging.download(
            self.app_id,
            'flows',
            self.flow_id,
            self.start,
            stop
        )
        self.assertEquals(resp.status_code, 200)
        self.assertTrue(len(resp.content) > 0)
