#!/usr/bin/env python2
import os
import time
import json
import unittest

import audi
from audi.common.ssh import SSHClient
from audi.common.api import StreamRestAPI
from audi.common.api import DatasetRestAPI


class FileDropZoneTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.stream_id = "logEventStream"

        self.username = audi.config['username']
        self.host = audi.config['cdap']['host']
        self.port = audi.cofnig['cdap']['port']
        self.base_url = "http://{0}:{1}/v2".format(self.host, self.port)

        self.stream = StreamRestAPI(self.base_url)
        self.dataset = DatasetRestAPI(self.base_url)
        self.ssh = SSHClient(self.host, self.username)

    def tearDown(self):
        self.dataset.delete_all()  # the only way to delete streams

    def test_service(self):
        target = "/tmp/file_drop_zone_test_file"
        destination = "/var/file-drop-zone/obs1"

        # read number of events before upload
        req = self.stream.read_events(self.stream_id)
        before = len(json.loads(req.content))

        # create dummy test file
        test_file = open(target, "w")
        test_file.write("Event 1")
        test_file.write("Event 2")
        test_file.write("Event 3")
        test_file.close()

        # upload file
        self.ssh.upload_file(target, destination)
        time.sleep(10)  # wait for the service to ingest the data

        # after upload
        req = self.stream.read_events(self.stream_id)
        after = len(json.loads(req.content))
        self.assertNotEquals(before, after)
        os.remove(target)
        os.remove(destination)
