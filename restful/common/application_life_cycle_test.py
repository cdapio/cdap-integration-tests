#!/usr/bin/env python2
import time
import unittest

import audi
from audi.cdap import ClientRestClient


class PurchaseHistoryLifeCycleTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'PurchaseHistory'
        self.flow_id = 'PurchaseFlow'
        self.mapreduce_id = 'PurchaseHistoryWorkflow_PurchaseHistoryBuilder'
        self.procedure_id = 'PurchaseProcedure'

        self.client = ClientRestClient()
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['Purchase'])

    @classmethod
    def tearDownClass(self):
        self.client.unrecoverable_reset()

    def test_deploy_status(self):
        found_app = False
        resp = self.client.list_apps()
        json_data = resp.json()

        for obj in json_data:
            if obj['name'] == self.app_id:
                found_app = True
                break

        self.assertTrue(found_app)
        self.assertEquals(resp.status_code, 200)

    def test_flow(self):
        # start flow
        time.sleep(10)
        resp = self.client.start_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(resp.status_code, 200)

        # start flow again
        time.sleep(2)
        resp = self.client.start_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(resp.status_code, 409)

        # stop flow
        time.sleep(2)
        resp = self.client.stop_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(resp.status_code, 200)

        # stop flow again - should raise conflict
        time.sleep(2)
        resp = self.client.stop_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(resp.status_code, 409)

    def test_mapreduce(self):
        # start flow
        resp = self.client.start_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(resp.status_code, 200)

        # start map reduce job
        time.sleep(2)
        resp = self.client.start_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(resp.status_code, 200)

        # check status of map reduce job
        time.sleep(5)
        resp = self.client.element_status(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(resp.json()['status'], 'RUNNING')
        self.assertEquals(resp.status_code, 200)

        # start map reduce job - should raise conflict
        time.sleep(5)
        resp = self.client.start_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(resp.status_code, 409)

        # stop map reduce job
        time.sleep(2)
        resp = self.client.stop_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(resp.status_code, 200)

        # stop map reduce job - should raise conflict
        time.sleep(2)
        resp = self.client.stop_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(resp.status_code, 409)

    def test_procedure(self):
        # start procedure
        resp = self.client.start_element(
            self.app_id,
            'procedures',
            self.procedure_id
        )
        self.assertEquals(resp.status_code, 200)

        # start procedure again - should raise conflict
        resp = self.client.start_element(
            self.app_id,
            'procedures',
            self.procedure_id
        )
        self.assertEquals(resp.status_code, 409)

        # stop procedure
        resp = self.client.stop_element(
            self.app_id,
            'procedures',
            self.procedure_id
        )
        self.assertEquals(resp.status_code, 200)

        # stop procedure again - should raise conflict
        resp = self.client.stop_element(
            self.app_id,
            'procedures',
            self.procedure_id
        )
        self.assertEquals(resp.status_code, 409)
