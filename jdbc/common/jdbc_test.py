#!/usr/bin/env python
import os
import time
import unittest

import audi
import audi.pentaho as kettle
from audi.cdap import ClientRestClient
from audi.cdap import StreamRestClient


class JDBCTest(unittest.TestCase):
    def setUp(self):
        self.guide_home = audi.config['cdap-bi-guide']['home']
        self.kettle_home = audi.config['pentaho']['home']
        self.client = ClientRestClient()
        self.stream = StreamRestClient()

        self.ktr_file = os.path.join(
            os.path.dirname(__file__),
            'ktr',
            'total_spend_per_customer.ktr'
        ).replace(' ', '\ ')

        # deploy apps
        self.client.deploy_app(audi.config['cdap-bi-guide']['jar'])
        self.client.start_element('PurchaseApp', 'flows', 'PurchaseFlow')

        # stream some events
        time.sleep(1)
        self.stream.send_event('purchases', 'Tom, 5, pear')
        self.stream.send_event('purchases', 'Alice, 12, apple')
        self.stream.send_event('purchases', 'Alice, 6, banana')
        self.stream.send_event('purchases', 'Bob, 2, orange')
        self.stream.send_event('purchases', 'Bob, 1, watermelon')
        self.stream.send_event('purchases', 'Bob, 10, apple')

    def tearDown(self):
        self.stream.truncate('purchases')
        self.client.stop_element(
            'PurchaseApp',
            'flow',
            'PurchaseApp.PurchaseFlow'
        )
        # self.client.unrecoverable_reset()

    def test_transformation(self):
        output_fp = '/tmp/test_output.txt'

        # run transformation
        kettle.modify_host_port(self.ktr_file)
        kettle.run_transformation(self.ktr_file)

        # assert
        self.assertTrue(os.path.isfile(output_fp))
        output_file = open(output_fp, 'r')
        test_output = output_file.read()
        output_file.close()

        self.assertEquals(
            test_output.split(),
            [
                'cdap_user_purchasesdataset.customer;total_spend',
                'Alice;66',
                'Bob;64',
                'Tom;10'
            ]
        )

        # clean up
        os.remove(output_fp)
