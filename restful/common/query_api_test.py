#!/usr/bin/env python2
import time
import json
import unittest

from audi.cdap import QueryRestClient


class QueryAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = "PurchaseHistory"
        self.query = QueryRestClient()

    def test_submit_query(self):
        req = self.query.submit('{"query": "SHOW TABLES"}')
        self.assertEquals(req.status_code, 200)

    def test_status_of_query(self):
        # submit query
        req = self.query.submit('{"query": "SHOW TABLES"}')
        time.sleep(2)  # sleep while query is performing

        # obtain status of query
        handle = json.loads(req.content)['handle']
        req = self.query.status(handle)
        status = json.loads(req.content)
        self.assertEquals(req.status_code, 200)
        self.assertEquals(status['status'], 'FINISHED')

    def test_obtain_results_schema(self):
        # submit query
        req = self.query.submit('{"query": "SHOW TABLES"}')
        time.sleep(2)  # sleep while query is performing

        # obtain results schema
        handle = json.loads(req.content)['handle']
        req = self.query.get_results_schema(handle)
        self.assertEquals(req.status_code, 200)

    def test_retreving_query_results(self):
        # submit query
        req = self.query.submit('{"query": "SHOW TABLES"}')
        time.sleep(2)  # sleep while query is performing

        # obtain query results
        handle = json.loads(req.content)['handle']
        req = self.query.get_results(handle, '{"size": 1}')
        self.assertEquals(req.status_code, 200)

    def test_close_query(self):
        # submit query
        req = self.query.submit('{"query": "SHOW TABLES"}')
        time.sleep(2)  # sleep while query is performing

        # close query
        handle = json.loads(req.content)['handle']
        req = self.query.close(handle)
        self.assertEquals(req.status_code, 200)

    def test_list_query(self):
        # submit query
        req = self.query.submit('{"query": "SHOW TABLES"}')

        # list query
        req = self.query.list_queries()
        self.assertEquals(req.status_code, 200)
        self.assertNotEquals(len(req.content), 0)

    def test_download_query_results(self):
        # submit query
        req = self.query.submit('{"query": "SHOW TABLES"}')
        handle = json.loads(req.content)['handle']
        time.sleep(2)  # sleep while query is performing

        # download query results
        req = self.query.download_results(handle)
        self.assertEquals(req.status_code, 200)

    # def test_hive_table_schema(self):
    #     # test hive table schema
    #     dataset_id = "history"
    #     req = self.query.get_hive_schema(dataset_id)
    #     if req != 200:
    #         print "ERROR: " + req.content

    #     self.assertEquals(req.status_code, 200)
