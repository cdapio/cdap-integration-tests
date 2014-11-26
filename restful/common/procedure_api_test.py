#!/usr/bin/env python
import time
import unittest

import audi
from audi.cdap import ClientRestClient
from audi.cdap import ProcedureRestClient


class ProcedureAPITest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app_id = 'WordCount'
        self.procedure = ProcedureRestClient()
        self.client = ClientRestClient()

        # deploy app
        cdap_examples = audi.config['cdap']['examples']
        self.client.deploy_app(cdap_examples['WordCount'])

    @classmethod
    def tearDownClass(self):
        self.client.unrecoverable_reset()

    def test_execute_procedures(self):
        app_id = 'WordCount'
        procedure_id = 'RetrieveCounts'
        method_id = 'getCount'

        # start procedure
        self.client.start_element(app_id, 'procedures', procedure_id)

        # execute procedure
        data = {'word': 'a'}
        req = self.procedure.execute(app_id, procedure_id, method_id, data)
        self.assertEquals(req.status_code, 200)

        # stop procedure
        self.client.stop_element(app_id, 'procedures', procedure_id)
        time.sleep(5)
