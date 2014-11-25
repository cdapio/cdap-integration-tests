#!/usr/bin/env python
import time
import json
import unittest

from audi.cdap import ClientRestClient


class ClientAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'HelloWorld'
        self.flow_id = 'WhoFlow'
        self.flowlet_id = 'saver'
        self.proc_id = 'Greeting'

        self.client = ClientRestClient()

    def test_list_apps(self):
        req = self.client.list_apps()
        self.assertEquals(req.status_code, 200)
        self.assertTrue(len(req.content) > 0)

    def test_details_of_app(self):
        req = self.client.app_specifications(self.app_id)
        self.assertEquals(req.status_code, 200)
        self.assertTrue(len(req.content) > 0)

    def test_runtime_arguments(self):
        # start flow
        time.sleep(2)
        req = self.client.start_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)

        # stop flow
        time.sleep(2)
        req = self.client.stop_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)

        # status of flow
        time.sleep(2)
        req = self.client.element_status(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)
        self.assertTrue(len(req.content) > 0)

    def test_container_info(self):
        app_id = 'WordCount'
        element_type = 'flows'
        element_id = 'WordCounter'

        req = self.client.container_info(app_id, element_type, element_id)
        self.assertEquals(req.status_code, 200)
        self.assertNotEquals(len(req.content), 0)

    def test_scale(self):
        data = [{
            'appId': 'PurchaseHistory',
            'programType': 'Flow',
            'programId': 'PurchaseFlow',
            'runnableId': 'collector'
        }]
        data = json.dumps(data)
        req = self.client.instance_info(data)
        self.assertEquals(req.status_code, 200)
        self.assertTrue(len(req.content) > len(data))

    def test_set_flowlet_instances(self):
        # query flowlet instances - before scale
        req = self.client.get_flowlet_instances(
            self.app_id,
            self.flow_id,
            self.flowlet_id
        )
        self.assertEquals(req.status_code, 200)
        self.assertEquals(json.loads(req.content)['instances'], 1)

        # scale flowlet instances
        req = self.client.set_flowlets(
            self.app_id,
            self.flow_id,
            self.flowlet_id, 2
        )
        self.assertEquals(req.status_code, 200)

        # query flowlet instances - after scale
        req = self.client.get_flowlet_instances(
            self.app_id,
            self.flow_id,
            self.flowlet_id
        )
        self.assertEquals(req.status_code, 200)
        self.assertEquals(json.loads(req.content)['instances'], 2)

    def test_set_procedure_instances(self):
        # query procedure instances - before scale
        req = self.client.get_procedure_instances(self.app_id, self.proc_id)
        self.assertEquals(req.status_code, 200)
        self.assertEquals(json.loads(req.content)['instances'], 1)

        # scale procedure instances
        req = self.client.set_procedures(self.app_id, self.proc_id, 2)
        self.assertEquals(req.status_code, 200)

        # query procedure instances - after scale
        req = self.client.get_procedure_instances(self.app_id, self.proc_id)
        self.assertEquals(req.status_code, 200)
        self.assertEquals(json.loads(req.content)['instances'], 2)

    def test_get_service_instances(self):
        app_id = 'PurchaseHistory'
        service_id = 'CatalogLookup'

        # query service instances - before scale
        req = self.client.get_service_instances(app_id, service_id)
        self.assertEquals(req.status_code, 200)
        self.assertEquals(json.loads(req.content)['requested'], 1)

        # scale
        req = self.client.set_services(app_id, service_id, 2)
        self.assertEquals(req.status_code, 200)

        # double check new instance number
        req = self.client.get_service_instances(app_id, service_id)
        self.assertEquals(req.status_code, 200)
        self.assertEquals(json.loads(req.content)['requested'], 2)

    def test_run_history_and_schedule(self):
        # start flow
        time.sleep(2)
        req = self.client.start_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)

        # stop flow
        time.sleep(2)
        req = self.client.stop_element(
            self.app_id,
            'flows',
            self.flow_id
        )
        self.assertEquals(req.status_code, 200)

        # query run history
        time.sleep(2)
        req = self.client.get_run_history(self.app_id, 'flows', self.flow_id)
        result = json.loads(req.content)
        self.assertEquals(req.status_code, 200)
        self.assertEquals(len(result), 1)

    def test_flow_liveinfo(self):
        # start debuggging for flow
        # resp = self.client.flow_debug('PurchaseHistory', 'PurchaseFlow')
        # self.assertEquals(resp.status_code, 200)

        # start flow
        time.sleep(2)
        resp = self.client.start_element(
            'PurchaseHistory',
            'flows',
            'PurchaseFlow'
        )
        self.assertEquals(resp.status_code, 200)

        # get liveinfo for flow
        resp = self.client.container_info(
            'PurchaseHistory',
            'flows',
            'PurchaseFlow'
        )
        self.assertTrue(len(resp.content) > 0)
        self.assertEquals(resp.status_code, 200)

        # stop flow
        time.sleep(2)
        resp = self.client.stop_element(
            'PurchaseHistory',
            'flows',
            'PurchaseFlow'
        )
        self.assertEquals(resp.status_code, 200)

        # check status
        time.sleep(2)
        resp = self.client.flow_status('PurchaseHistory', 'PurchaseFlow')
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(resp.json()['status'], 'STOPPED')

    def test_procedure_liveinfo(self):
        # resp = self.client.procedure_debug(
        #     'PurchaseHistory',
        #     'PurchaseProcedure'
        # )
        # self.assertEquals(resp.status_code, 200)

        # start procedure
        time.sleep(2)
        resp = self.client.start_element(
            'PurchaseHistory',
            'procedures',
            'PurchaseProcedure'
        )
        self.assertEquals(resp.status_code, 200)

        # get liveinfo for procedure
        resp = self.client.container_info(
            'PurchaseHistory',
            'procedures',
            'PurchaseProcedure'
        )
        self.assertTrue(len(resp.content) > 0)
        self.assertEquals(resp.status_code, 200)

        # stop procedure
        time.sleep(2)
        resp = self.client.stop_element(
            'PurchaseHistory',
            'procedures',
            'PurchaseProcedure'
        )
        self.assertEquals(resp.status_code, 200)

        # check status
        time.sleep(2)
        resp = self.client.procedure_status(
            'PurchaseHistory',
            'PurchaseProcedure'
        )
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(resp.json()['status'], 'STOPPED')
