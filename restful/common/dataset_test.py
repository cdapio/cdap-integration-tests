#!/usr/bin/env python2
import json
import time
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import DatasetRestClient


class DatasetTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'PurchaseHistory'
        self.flow_id = 'PurchaseFlow'
        self.mapreduce_id = 'PurchaseHistoryWorkflow_PurchaseHistoryBuilder'
        self.procedure_id = 'PurchaseProcedure'
        self.dataset_id = 'testTable'

        self.client = ClientRestClient()
        self.dataset = DatasetRestClient()

    @classmethod
    def tearDownClass(self):
        audi.stop_app(self.app_id)
        self.client.unrecoverable_reset()
        time.sleep(5)

    def test_create_dataset(self):
        # should not create a dataset with invalid type
        details = {
            'typeName': 'co.cask.cdap.api.dataset.keyvalue.Table',
            'properties': {'ttl': '3600'}
        }
        req = self.dataset.create(self.dataset_id, details)
        self.assertEquals(req.status_code, 404)

        # create a dataset
        details = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600'}
        }
        req = self.dataset.create(self.dataset_id, details)
        self.assertEquals(req.status_code, 200)

        # create the same dataset - should raise error
        req = self.dataset.create(self.dataset_id, details)
        self.assertEquals(req.status_code, 409)

        # list datasets
        req = self.dataset.list_datasets()
        self.assertTrue(len(req.content) > 0)
        self.assertEquals(req.status_code, 200)

        # cleanup
        self.dataset.delete(self.dataset_id)

    def test_truncate_dataset(self):
        # create a dataset
        details = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600'}
        }
        req = self.dataset.create(self.dataset_id, details)
        self.assertEquals(req.status_code, 200)

        # truncate dataset
        req = self.dataset.truncate(self.dataset_id)
        self.assertEquals(req.status_code, 200)

        # cleanup
        self.dataset.delete(self.dataset_id)

    def test_update_dataset(self):
        # create a dataset
        details = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600'}
        }
        req = self.dataset.create(self.dataset_id, details)
        self.assertEquals(req.status_code, 200)

        # update dataset
        details = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '7200'}
        }
        req = self.dataset.update(self.dataset_id, details)
        self.assertEquals(req.status_code, 200)

        # cleanup
        self.dataset.delete(self.dataset_id)

    def test_delete_dataset(self):
        # create a dataset
        data = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600000'}
        }
        req = self.dataset.create(self.dataset_id, data)

        # before delete
        req = self.dataset.list_datasets()
        before = len(json.loads(req.content))

        # delete a dataset
        req = self.dataset.delete(self.dataset_id)
        self.assertEquals(req.status_code, 200)

        # after delete
        req = self.dataset.list_datasets()
        after = len(json.loads(req.content))
        self.assertNotEquals(before, after)

        # delete a non-existing  dataset
        req = self.dataset.delete(self.dataset_id)
        self.assertEquals(req.status_code, 404)
