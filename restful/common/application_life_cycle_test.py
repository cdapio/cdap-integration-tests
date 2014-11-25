#!/usr/bin/env python2
import time
import unittest

from audi.cdap import ClientRestClient


class PurchaseHistoryLifeCycleTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'PurchaseHistory'
        self.flow_id = 'PurchaseFlow'
        self.mapreduce_id = 'PurchaseHistoryWorkflow_PurchaseHistoryBuilder'
        self.procedure_id = 'PurchaseProcedure'

        # deploy apps
        self.client = ClientRestClient()

    def test_deploy_status(self):
        found_app = False
        req = self.client.list_apps()
        json_data = req.json()

        for obj in json_data:
            if obj['name'] == self.app_id:
                found_app = True
                break

        self.assertTrue(found_app)
        self.assertEquals(req.status_code, 200)
        self.assertTrue(len(json_data) > 1)

    def test_flow(self):
        # start flow
        time.sleep(2)
        req = self.client.start_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(req.status_code, 200)

        # start flow again
        time.sleep(2)
        req = self.client.start_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(req.status_code, 409)

        # stop flow
        time.sleep(2)
        req = self.client.stop_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(req.status_code, 200)

        # stop flow again - should raise conflict
        time.sleep(2)
        req = self.client.stop_element(self.app_id, 'flows', self.flow_id)
        self.assertEquals(req.status_code, 409)

    def test_mapreduce(self):
        # start flow
        req = self.client.start_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)

        # start map reduce job
        time.sleep(2)
        req = self.client.start_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(req.status_code, 200)

        # check status of map reduce job
        time.sleep(20)
        req = self.client.element_status(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(req.json()['status'], 'RUNNING')
        self.assertEquals(req.status_code, 200)

        # start map reduce job - should raise conflict
        time.sleep(2)
        req = self.client.start_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(req.status_code, 409)

        # stop map reduce job
        time.sleep(2)
        req = self.client.stop_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(req.status_code, 200)

        # stop map reduce job - should raise conflict
        time.sleep(2)
        req = self.client.stop_element(
            self.app_id,
            'mapreduce',
            self.mapreduce_id
        )
        self.assertEquals(req.status_code, 409)

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
