#!/usr/bin/env python
import os
import requests
import unittest

import audi
from audi.cdap import ClientRestClient


@unittest.skipIf(audi.config['cdap']['secure'] is False, 'skip test')
class AuthTest(unittest.TestCase):
    def setUp(self):
        # temporarily remove auth header from config
        self.auth_header = audi.config['cdap']['authorization']
        audi.config['cdap']['authorization'] = None

    def tearDown(self):
        # restore auth header
        audi.config['cdap']['authorization'] = self.auth_header

    def test_invalid_access_no_token(self):
        client = ClientRestClient()
        resp = client.list_apps()
        self.assertEquals(resp.status_code, 401)
        self.assertTrue('auth_uri' in resp.json())

    def test_invalid_access_invalid_token(self):
        audi.config['cdap']['authorization'] = {
            'Authorization': 'Bearer INVALID_TOKEN'
        }
        client = ClientRestClient()
        resp = client.list_apps()
        self.assertEquals(resp.status_code, 401)
        self.assertTrue('auth_uri' in resp.json())

    def test_valid_access(self):
        # get auth url
        client = ClientRestClient()
        resp = client.list_apps()
        self.assertEquals(resp.status_code, 401)
        self.assertTrue('auth_uri' in resp.json())

        # get access token
        auth_url = resp.json()['auth_uri'][0]
        auth = (audi.config.get('username'), audi.config.get('password'))
        if None in auth:
            err = 'Error! You have not provided username and/or password!'
            raise RuntimeError(err)

        resp = requests.get(auth_url, auth=auth)
        auth_data = resp.json()
        self.assertEquals(resp.status_code, 200)
        self.assertTrue('access_token' in auth_data)
        self.assertTrue('token_type' in auth_data)
        self.assertTrue('expires_in' in auth_data)

        # try get app list
        client.auth = {
            'Authorization': '{0} {1}'.format(
                auth_data['token_type'],
                auth_data['access_token']
            )
        }
        resp = client.list_apps()
        self.assertEquals(resp.status_code, 200)

    def test_get_access_token(self):
        # get auth url
        client = ClientRestClient()
        resp = client.list_apps()
        self.assertEquals(resp.status_code, 401)
        self.assertTrue('auth_uri' in resp.json())

        # get access token
        auth_url = resp.json()['auth_uri'][0]
        auth = (audi.config.get('username'), audi.config.get('password'))
        if None in auth:
            err = 'Error! You have not provided username and/or password!'
            raise RuntimeError(err)

        resp = requests.get(auth_url, auth=auth)
        auth_data = resp.json()
        self.assertEquals(resp.status_code, 200)
        self.assertTrue('access_token' in auth_data)
        self.assertTrue('token_type' in auth_data)
        self.assertTrue('expires_in' in auth_data)
