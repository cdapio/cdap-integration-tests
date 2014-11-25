#!/usr/bin/env python
import unittest

from audi.cdap import MonitorRestClient


class MonitorAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'HelloWorld'
        self.monitor = MonitorRestClient()

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
