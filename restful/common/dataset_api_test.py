#!/usr/bin/env python2
import json
import unittest

from audi.cdap import DatasetRestClient


class DatasetAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dataset_id = 'mydataset'
        self.datasets = DatasetRestClient()

    def setUp(self):
        # remove if dataset exists
        self.datasets.delete(self.dataset_id)

    def test_list_datasets(self):
        req = self.datasets.list_datasets()
        self.assertNotEquals(len(req.content), 0)

    def test_create_dataset(self):
        # before create
        req = self.datasets.list_datasets()
        before = len(json.loads(req.content))

        # test create dataset
        data = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600000'}
        }
        req = self.datasets.create(self.dataset_id, json.dumps(data))
        self.assertEquals(req.status_code, 200)

        # after create
        req = self.datasets.list_datasets()
        after = len(json.loads(req.content))
        self.assertNotEquals(before, after)

    def test_update_dataset(self):
        # create dataset
        data = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600000'}
        }
        resp = self.datasets.create(self.dataset_id, json.dumps(data))

        # before update
        resp = self.datasets.list_datasets()
        before = None
        for obj in resp.json():
            if obj["name"] == "cdap.user." + self.dataset_id:
                before = obj
                break
        ttl_before = before['properties']['ttl']

        # update dataset
        data = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '100000'}
        }
        resp = self.datasets.update(self.dataset_id, json.dumps(data))
        self.assertEquals(resp.status_code, 200)

        # after update
        resp = self.datasets.list_datasets()
        after = None
        for obj in resp.json():
            if obj["name"] == "cdap.user." + self.dataset_id:
                after = obj
                break
        ttl_after = after['properties']['ttl']

        self.assertNotEquals(ttl_before, ttl_after)

    def test_delete_a_dataset(self):
        # create a dataset
        data = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600000'}
        }
        req = self.datasets.create(self.dataset_id, json.dumps(data))

        # before delete
        req = self.datasets.list_datasets()
        before = len(json.loads(req.content))

        # delete a dataset
        req = self.datasets.delete(self.dataset_id)
        self.assertEquals(req.status_code, 200)

        # after delete
        req = self.datasets.list_datasets()
        after = len(json.loads(req.content))
        self.assertNotEquals(before, after)

    # def test_delete_all_dataset(self):
    #     # create a dataset
    #     data = {
    #         'typeName': 'co.cask.cdap.api.dataset.table.Table',
    #         'properties': {'ttl': '3600000'}
    #     }
    #     req = self.datasets.create(self.dataset_id, json.dumps(data))

    #     # before delete
    #     req = self.datasets.list_datasets()
    #     before = len(json.loads(req.content))

    #     # delete all datasets
    #     req = self.datasets.delete_all()
    #     self.assertEquals(req.status_code, 200)

    #     # after delete
    #     req = self.datasets.list_datasets()
    #     after = len(json.loads(req.content))
    #     self.assertNotEquals(before, after)

    def test_truncate_dataset(self):
        # create a dataset
        data = {
            'typeName': 'co.cask.cdap.api.dataset.table.Table',
            'properties': {'ttl': '3600000'}
        }
        req = self.datasets.create(self.dataset_id, json.dumps(data))

        # truncate a dataset
        req = self.datasets.truncate(self.dataset_id)
        self.assertEquals(req.status_code, 200)
