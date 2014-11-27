#!/usr/bin/env python2
import time
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import ServiceRestClient


class GenericAppTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'PurchaseHistory'
        self.client = ClientRestClient()
        self.service = ServiceRestClient()

        # deploy app
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['Purchase'])

    @classmethod
    def tearDownClass(self):
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def test_flows(self):
        # check there are apps
        resp = self.client.list_apps()
        self.assertTrue(len(resp.json()) >= 1)

        # get flow details
        flow_ids = []
        resp = self.client.flows(self.app_id)
        data = resp.json()
        for el in data:
            flow_ids.append(el['id'])

        self.assertEquals(resp.status_code, 200)
        self.assertEquals(flow_ids, ['PurchaseFlow'])

        # start flow(s)
        for flow_id in flow_ids:
            resp = self.client.start_element(
                self.app_id,
                'flows',
                flow_id
            )
            self.assertEquals(resp.status_code, 200)

        # check flow status
        time.sleep(5)
        for flow_id in flow_ids:
            resp = self.client.flow_status(self.app_id, flow_id)
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()['status'], 'RUNNING')

        # stop flow(s)
        for flow_id in flow_ids:
            resp = self.client.stop_element(self.app_id, 'flows', flow_id)
            self.assertEquals(resp.status_code, 200)

        # check flow status
        time.sleep(5)
        for flow_id in flow_ids:
            resp = self.client.flow_status(self.app_id, flow_id)
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()['status'], 'STOPPED')

    def test_services(self):
        resp = self.service.list_services(self.app_id)
        services = resp.json()
        self.assertEquals(resp.status_code, 200)
        self.assertTrue(len(services) > 0)

        # start services
        for service in services:
            resp = self.client.start_element(
                self.app_id,
                'services',
                service['name']
            )
            self.assertEquals(resp.status_code, 200)

        # start services - should raise conflict
        for service in services:
            resp = self.client.start_element(
                self.app_id,
                'services',
                service['name']
            )
            self.assertEquals(resp.status_code, 409)

        # check flow status
        time.sleep(5)
        for service in services:
            resp = self.service.status(self.app_id, service['name'])
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()['status'], 'RUNNING')

        # stop services
        for service in services:
            resp = self.client.stop_element(
                self.app_id,
                'services',
                service['name']
            )
            self.assertEquals(resp.status_code, 200)

        # stop services - should raise conflict
        for service in services:
            resp = self.client.stop_element(
                self.app_id,
                'services',
                service['name']
            )
            self.assertEquals(resp.status_code, 409)
