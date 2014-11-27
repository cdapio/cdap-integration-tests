#!/usr/bin/env python2
import time
import random
import unittest

import audi
from audi.cdap import ServiceRestClient
from audi.cdap import StreamRestClient
from audi.cdap import ClientRestClient


class ServiceAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.stream_id = 'purchaseStream'
        self.app_id = 'PurchaseHistory'
        self.flow_id = 'PurchaseFlow'
        self.service_id = 'CatalogLookup'
        self.method_id = 'v1/product/abc/catalog'

        self.service = ServiceRestClient()
        self.client = ClientRestClient()

        # deploy app
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['Purchase'])

    @classmethod
    def tearDownClass(self):
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def setUp(self):
        # start flow
        req = self.client.start_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)
        time.sleep(2)

        # start service
        req = self.client.start_element(
            self.app_id,
            'services',
            self.service_id
        )
        self.assertEquals(req.status_code, 200)
        time.sleep(2)

        # send evenets to stream
        name = ['chris', 'sree', 'poorna']
        product = ['iphone', 'iwatch', 'macbook']
        stream = StreamRestClient()

        for i in range(100):
            data = '{0} bought {1} {2} for ${3}'.format(
                random.sample(name, 1)[0],
                random.randint(0, 10),
                random.sample(product, 1)[0],
                random.randint(0, 1000000)
            )
            stream.send_event(self.stream_id, data)
        time.sleep(3)

    def tearDown(self):
        # stop flow
        req = self.client.stop_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)
        time.sleep(2)

        # stop service
        req = self.client.stop_element(
            self.app_id,
            'services',
            self.service_id
        )
        self.assertEquals(req.status_code, 200)
        time.sleep(2)

    def test_service(self):
        req = self.service.request_method(
            'GET',
            self.app_id,
            self.service_id,
            self.method_id
        )
        self.assertEquals(req.status_code, 200)
        self.assertEquals(req.content, 'Catalog-abc')
