#!/usr/bin/env python
import time
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import MonitorRestClient


class MonitorAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'HelloWorld'
        self.monitor = MonitorRestClient()
        self.client = ClientRestClient()

        # deploy app
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['HelloWorld'])

    @classmethod
    def tearDownClass(self):
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def test_list_all_services(self):
        resp = self.monitor.all_service_info()
        self.assertEquals(resp.status_code, 200)
        self.assertTrue(len(resp.content) > 0)

    def test_check_status_of_all_services(self):
        resp = self.monitor.all_service_status()
        self.assertEquals(resp.status_code, 200)
        self.assertTrue(len(resp.content) > 0)

    def test_check_status_of_a_service(self):
        resp = self.monitor.service_status('streams')
        self.assertEquals(resp.status_code, 200)
        self.assertTrue(len(resp.content) > 0)

    def test_query_system_service_instances(self):
        resp = self.monitor.system_service_instances('streams')
        self.assertEquals(resp.status_code, 200)
        self.assertTrue(len(resp.content) > 0)

    # def test_set_system_service_instances(self):
    #     # query number of instances
    #     resp = self.monitor.system_service_instances('streams')
    #     instances_before = resp.json()['provisioned']

    #     # scale instances
    #     resp = self.monitor.set_system_service('streams', 2)
    #     time.sleep(2)

    #     # query number of instances
    #     resp = self.monitor.system_service_instances('streams')
    #     instances_after = resp.json()['requested']
    #     self.assertNotEquals(instances_before, instances_after)
