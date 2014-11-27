#!/usr/bin/env python
import time
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import MetricsRestClient
from audi.cdap import DatasetRestClient


class MetricsAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'HelloWorld'
        self.flow_id = 'WhoFlow'
        self.flowlet_id = 'saver'
        self.service_id = 'WhoService'
        self.runnable_id = 'WhoRun'

        self.metrics = MetricsRestClient()
        self.dataset = DatasetRestClient()
        self.client = ClientRestClient()

        # deploy app
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['HelloWorld'])

        # start flow
        time.sleep(2)
        req = self.client.start_element(self.app_id, 'flows', self.flow_id)
        if req.status_code != 200:
            raise RuntimeError('Failed to start flow!')

    @classmethod
    def tearDownClass(self):
        # stop flow
        time.sleep(2)
        req = self.client.stop_element(self.app_id, 'flows', self.flow_id)
        if req.status_code != 200:
            raise RuntimeError('Failed to stop flow!')

        # stop app
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def test_metric_request(self):
        # test system metric
        scope = 'system'
        context = 'apps/HelloWorld/flows/WhoFlow/flowlets/saver'
        metric = 'process.busyness'
        req = self.metrics.metric_request(scope, context, metric)
        self.assertEquals(req.status_code, 200)
        self.assertTrue(len(req.content) > 0)

        # using a User-Defined metric, names.bytes
        scope = 'user'
        context = 'apps/HelloWorld/flows/WhoFlow/flowlets/saver'
        metric = 'name.bytes'
        req = self.metrics.metric_request(scope, context, metric)
        self.assertEquals(req.status_code, 200)
        self.assertTrue(len(req.content) > 0)
