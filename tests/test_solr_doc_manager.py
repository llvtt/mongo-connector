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

import datetime
import sys
import time

sys.path[0:0] = [""]

from mongo_connector.command_helper import CommandHelper
from mongo_connector.doc_managers.solr_doc_manager import DocManager

from tests import unittest, TESTARGS
from tests.test_gridfs_file import MockGridFSFile
from tests.test_solr import SolrTestCase


class TestSolrDocManager(SolrTestCase):
    """Test class for SolrDocManager
    """

    def setUp(self):
        """Empty Solr at the start of every test
        """
        self._remove()

    def test_update(self):
        doc_id = '1'
        doc = {"_id": doc_id, "title": "abc", "description": "def"}
        self.docman.upsert(doc, *TESTARGS)
        # $set only
        update_spec = {"$set": {"title": "qaz", "description": "wsx"}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        expected = {"_id": doc_id, "title": "qaz", "description": "wsx"}
        # We can't use assertEqual here, because Solr adds some
        # additional fields like _version_ to all documents
        for k, v in expected.items():
            self.assertEqual(doc[k], v)

        # $unset only
        update_spec = {"$unset": {"title": True}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        expected = {"_id": '1', "description": "wsx"}
        for k, v in expected.items():
            self.assertEqual(doc[k], v)
        self.assertNotIn("title", doc)

        # mixed $set/$unset
        update_spec = {"$unset": {"description": True},
                       "$set": {"subject": "edc"}}
        doc = self.docman.update(doc_id, update_spec, *TESTARGS)
        expected = {"_id": '1', "subject": "edc"}
        for k, v in expected.items():
            self.assertEqual(doc[k], v)
        self.assertNotIn("description", doc)

    def test_upsert(self):
        """Ensure we can properly insert into Solr via DocManager.
        """
        #test upsert
        docc = {'_id': '1', 'name': 'John'}
        self.docman.upsert(docc, *TESTARGS)
        res = self.solr_conn.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'John')

        docc = {'_id': '1', 'name': 'Paul'}
        self.docman.upsert(docc, *TESTARGS)
        res = self.solr_conn.search('*:*')
        for doc in res:
            self.assertTrue(doc['_id'] == '1' and doc['name'] == 'Paul')

    def test_bulk_upsert(self):
        """Ensure we can properly insert many documents at once into
        Solr via DocManager

        """
        self.docman.bulk_upsert([], *TESTARGS)

        docs = ({"_id": i} for i in range(1000))
        self.docman.bulk_upsert(docs, *TESTARGS)

        res = sorted(int(x["_id"])
                     for x in self.solr_conn.search("*:*", rows=1001))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r, i)

        docs = ({"_id": i, "weight": 2*i} for i in range(1000))
        self.docman.bulk_upsert(docs, *TESTARGS)

        res = sorted(int(x["weight"])
                     for x in self.solr_conn.search("*:*", rows=1001))
        self.assertEqual(len(res), 1000)
        for i, r in enumerate(res):
            self.assertEqual(r, 2*i)

    def test_remove(self):
        """Ensure we can properly delete from Solr via DocManager.
        """
        #test remove
        docc = {'_id': '1', 'name': 'John'}
        self.docman.upsert(docc, *TESTARGS)
        res = self.solr_conn.search('*:*')
        self.assertTrue(len(res) == 1)

        self.docman.remove(docc['_id'], *TESTARGS)
        res = self.solr_conn.search('*:*')
        self.assertTrue(len(res) == 0)

    def test_insert_file(self):
        """Ensure we can properly insert a file into Solr via DocManager.
        """
        test_data = ' '.join(str(x) for x in range(100000))
        docc = {
            '_id': 'test_id',
            'filename': 'test_filename',
            'upload_date': datetime.datetime.now(),
            'md5': 'test_md5'
        }
        self.docman.insert_file(MockGridFSFile(docc, test_data), *TESTARGS)
        res = self.solr_conn.search('*:*')
        for doc in res:
            self.assertEqual(doc['_id'], docc['_id'])
            self.assertEqual(doc['filename'], docc['filename'])
            self.assertEqual(doc['content'][0].strip(),
                             test_data.strip())

    def test_remove_file(self):
        test_data = 'hello world'
        docc = {
            '_id': 'test_id',
            'filename': 'test_filename',
            'upload_date': datetime.datetime.now(),
            'md5': 'test_md5'
        }

        self.docman.insert_file(MockGridFSFile(docc, test_data), *TESTARGS)
        res = self.solr_conn.search('*:*')
        self.assertEqual(len(res), 1)

        self.docman.remove(docc['_id'], *TESTARGS)
        res = self.solr_conn.search('*:*')
        self.assertEqual(len(res), 0)

    def test_search(self):
        """Query Solr for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """
        #test search
        docc = {'_id': '1', 'name': 'John'}
        self.docman.upsert(docc, 'test.test', 5767301236327972865)
        docc = {'_id': '2', 'name': 'John Paul'}
        self.docman.upsert(docc, 'test.test', 5767301236327972866)
        docc = {'_id': '3', 'name': 'Paul'}
        self.docman.upsert(docc, 'test.test', 5767301236327972870)
        search = list(self.docman.search(5767301236327972865,
                                         5767301236327972866))
        search2 = list(self.solr_conn.search('John'))
        self.assertTrue(len(search) == len(search2))
        self.assertTrue(len(search) != 0)

        result_names = [result.get("name") for result in search]
        self.assertIn('John', result_names)
        self.assertIn('John Paul', result_names)

    def test_solr_commit(self):
        """Test that documents get properly added to Solr.
        """
        docc = {'_id': '3', 'name': 'Waldo'}
        docman = DocManager("http://localhost:8983/solr")
        # test cases:
        # -1 = no autocommit
        # 0 = commit immediately
        # x > 0 = commit within x seconds
        for autocommit_interval in [None, 0, 1, 2]:
            docman.auto_commit_interval = autocommit_interval
            docman.upsert(docc, *TESTARGS)
            if autocommit_interval is None:
                docman.commit()
            else:
                # Allow just a little extra time
                time.sleep(autocommit_interval + 1)
            results = list(self._search("Waldo"))
            self.assertEqual(len(results), 1,
                             "should commit document with "
                             "auto_commit_interval = %s" % str(
                                 autocommit_interval))
            self.assertEqual(results[0]["name"], "Waldo")
            self._remove()

    def test_get_last_doc(self):
        """Insert documents, Verify the doc with the latest timestamp.
        """
        #test get last doc
        docc = {'_id': '4', 'name': 'Hare'}
        self.docman.upsert(docc, 'test.test', 2)
        docc = {'_id': '5', 'name': 'Tortoise'}
        self.docman.upsert(docc, 'test.test', 1)
        doc = self.docman.get_last_doc()
        self.assertTrue(doc['_id'] == '4')

        docc = {'_id': '6', 'name': 'HareTwin', 'ts': '2'}
        doc = self.docman.get_last_doc()
        self.assertTrue(doc['_id'] == '4' or doc['_id'] == '6')

    def test_commands(self):
        self.docman.command_helper = CommandHelper()

        def count_ns(ns):
            return sum(1 for _ in self._search("ns:%s" % ns))

        self.docman.upsert({'_id': '1', 'test': 'data'}, *TESTARGS)
        self.assertEqual(count_ns("test.test"), 1)

        self.docman.handle_command({'drop': 'test'}, *TESTARGS)
        time.sleep(1)
        self.assertEqual(count_ns("test.test"), 0)

        self.docman.upsert({'_id': '2', 'test': 'data'}, 'test.test2', '2')
        self.docman.upsert({'_id': '3', 'test': 'data'}, 'test.test3', '3')
        self.docman.handle_command({'dropDatabase': 1}, 'test.$cmd', 1)
        time.sleep(1)
        self.assertEqual(count_ns("test.test2"), 0)
        self.assertEqual(count_ns("test.test3"), 0)


if __name__ == '__main__':
    unittest.main()
