#!/usr/bin/env python2
import unittest


class DummyTest(unittest.TestCase):
    def test_dummy(self):
        print "Running test dummy"
        self.assertEquals(1, 1)
