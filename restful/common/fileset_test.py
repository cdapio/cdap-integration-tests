#!/usr/bin/env python
import unittest
import requests
import audi

class FileSetTest(unittest.TestCase):
    def setUp(self):
        audi.deploy_app(__file__, 'FileSetExample.jar')
        audi.cdap.obtain_access_token()

        audi.cdap.prep_access_token()

    def tearDown(self):
        audi.delete_app('FileSetExample')

    def test_rest_endpoints(self):
        print list_datasets()
