#!/usr/bin/env python
import os
import unittest

import pexpect

import audi
import audi.cdap as cdap


class CDAPCLITest(unittest.TestCase):
    def setUp(self):
        cdap_home = audi.config['cdap']['home']
        self.cli_path = os.path.join(cdap_home, "cdap-cli/bin/cdap-cli.sh")

    def test_help(self):
        try:
            shell = pexpect.spawn("sh " + self.cli_path)
            shell.expect("cdap *", 4)
            shell.send("help\n")
            shell.expect("cdap *", 1)
            shell.close()
        except:
            raise
