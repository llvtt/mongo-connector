# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests methods in util.py
"""
import sys

from bson import timestamp

sys.path[0:0] = [""]

from mongo_connector.util import (bson_ts_to_long,
                                  long_to_bson_ts,
                                  retry_until_ok,
                                  retrieve_field,
                                  set_field)
from tests import unittest


def err_func():
    """Helper function for retry_until_ok test
    """

    err_func.counter += 1
    if err_func.counter == 3:
        return True
    else:
        raise TypeError

err_func.counter = 0


class TestUtil(unittest.TestCase):
    """ Tests the utils
    """

    def test_bson_ts_to_long(self):
        """Test bson_ts_to_long and long_to_bson_ts
        """

        tstamp = timestamp.Timestamp(0x12345678, 0x90abcdef)

        self.assertEqual(0x1234567890abcdef,
                         bson_ts_to_long(tstamp))
        self.assertEqual(long_to_bson_ts(0x1234567890abcdef),
                         tstamp)

    def test_retry_until_ok(self):
        """Test retry_until_ok
        """

        self.assertTrue(retry_until_ok(err_func))
        self.assertEqual(err_func.counter, 3)

    def test_retrieve_field(self):
        test_document = {
            'outer': 'space',
            'nested0': {
                'nested1': {
                    'greeting': 'hello'
                }
            }
        }
        # Key does not exist.
        self.assertRaises(KeyError, retrieve_field, ['garbage'], test_document)
        # Retrieve top-level key.
        self.assertEqual('space', retrieve_field(['outer'], test_document))
        # Retrieve nested key.
        self.assertEqual(
            'hello',
            retrieve_field(['nested0', 'nested1', 'greeting'], test_document))

    def test_set_field(self):
        test_document = {
            'outer': 'space',
            'nested0': {
                'nested1': {
                    'greeting': 'hello'
                }
            }
        }

        def set_and_assert(path, value):
            set_field(path, test_document, value)
            self.assertEqual(value, retrieve_field(path, test_document))

        # Replace existing field.
        set_and_assert(['outer'], 'limits')
        # Replace existing field, turning it into a dictionary.
        set_and_assert(['outer', 'limits'], 'aliens')
        # Set nested field, merging it into an existing dictionary.
        set_and_assert(['nested0', 'nested1', 'exclamation'], 'Gadzooks!')


if __name__ == '__main__':

    unittest.main()
