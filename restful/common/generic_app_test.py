#!/usr/bin/env python2
import time
import unittest

from audi.cdap import ClientRestClient
# from audi.cdap import DatasetRestClient


class GenericAppTest(unittest.TestCase):
    def setUp(self):
        self.app_id = 'PurchaseHistory'
        self.client = ClientRestClient()

    def test_flows(self):
        # check there is more than 1 app running
        resp = self.client.list_apps()
        self.assertTrue(len(resp.json()) > 1)

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
            resp = self.client.start_element(self.app_id, 'flows', flow_id)
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

    def test_procedures(self):
        # check there is more than 1 app running
        resp = self.client.list_apps()
        self.assertTrue(len(resp.json()) > 1)

        # get procedure details
        procedure_ids = []
        resp = self.client.procedures(self.app_id)
        data = resp.json()
        for el in data:
            procedure_ids.append(el['id'])

        self.assertEquals(resp.status_code, 200)
        self.assertEquals(procedure_ids, ['PurchaseProcedure'])

        # start procedure(s)
        for procedure_id in procedure_ids:
            resp = self.client.start_element(
                self.app_id,
                'procedures',
                procedure_id
            )
            self.assertEquals(resp.status_code, 200)

        # check procedure status
        time.sleep(5)
        for procedure_id in procedure_ids:
            resp = self.client.procedure_status(self.app_id, procedure_id)
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()['status'], 'RUNNING')

        # stop procedure(s)
        for procedure_id in procedure_ids:
            resp = self.client.stop_element(
                self.app_id,
                'procedures',
                procedure_id
            )
            self.assertEquals(resp.status_code, 200)

        # check procedure status
        time.sleep(5)
        for procedure_id in procedure_ids:
            resp = self.client.procedure_status(self.app_id, procedure_id)
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()['status'], 'STOPPED')
