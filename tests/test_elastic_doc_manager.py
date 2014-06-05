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

"""Tests each of the functions in elastic_doc_manager
"""

import time
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
from tests import elastic_pair
from tests.test_elastic import ElasticsearchTestCase

sys.path[0:0] = [""]

from mongo_connector.doc_managers.elastic_doc_manager import DocManager


class ElasticDocManagerTester(ElasticsearchTestCase):
    """Test class for elastic_docManager
    """

    def put_metadata(self, d):
        """Simulate the metadata that's included in documents passed on to
        DocManagers from an OplogThread."""
        d['ns'] = 'test.test'
        d['_ts'] = 1
        return d

    def test_update(self):
        doc = {"_id": '1', "a": 1, "b": 2}
        self.elastic_doc.upsert(self.put_metadata(doc))
        # $set only
        update_spec = {"$set": {"a": 1, "b": 2}}
        doc = self.elastic_doc.update(self.put_metadata(doc), update_spec)
        self.assertEqual(doc, {"_id": '1', "a": 1, "b": 2})
        # $unset only
        update_spec = {"$unset": {"a": True}}
        doc = self.elastic_doc.update(self.put_metadata(doc), update_spec)
        self.assertEqual(doc, {"_id": '1', "b": 2})
        # mixed $set/$unset
        update_spec = {"$unset": {"b": True}, "$set": {"c": 3}}
        doc = self.elastic_doc.update(self.put_metadata(doc), update_spec)
        self.assertEqual(doc, {"_id": '1', "c": 3})

    def test_upsert(self):
        """Ensure we can properly insert into ElasticSearch via DocManager.
        """

        docc = {'_id': '1', 'name': 'John'}
        self.elastic_doc.upsert(self.put_metadata(docc))
        res = self.elastic_conn.search(
            index="test.test",
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]
        for doc in res:
            self.assertEqual(doc['_id'], '1')
            self.assertEqual(doc['_source']['name'], 'John')

    def test_bulk_upsert(self):
        """Ensure we can properly insert many documents at once into
        ElasticSearch via DocManager.

        """
        self.elastic_doc.bulk_upsert([])

        docs = (self.put_metadata({"_id": i}) for i in range(1000))
        self.elastic_doc.bulk_upsert(docs)
        res = self.elastic_conn.search(
            index="test.test",
            body={"query": {"match_all": {}}},
            size=1001
        )["hits"]["hits"]
        returned_ids = sorted(int(doc["_id"]) for doc in res)
        self.assertEqual(len(returned_ids), 1000)
        for i, r in enumerate(returned_ids):
            self.assertEqual(r, i)

        docs = (self.put_metadata({"_id": i, "weight": 2*i})
                for i in range(1000))
        self.elastic_doc.bulk_upsert(docs)

        res = self.elastic_conn.search(
            index="test.test",
            body={"query": {"match_all": {}}},
            size=1001
        )["hits"]["hits"]
        returned_ids = sorted(int(doc["_source"]["weight"]) for doc in res)
        self.assertEqual(len(returned_ids), 1000)
        for i, r in enumerate(returned_ids):
            self.assertEqual(r, 2*i)

    def test_remove(self):
        """Ensure we can properly delete from ElasticSearch via DocManager.
        """

        docc = {'_id': '1', 'name': 'John'}
        self.elastic_doc.upsert(self.put_metadata(docc))
        res = self.elastic_conn.search(
            index="test.test",
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]
        res = [x["_source"] for x in res]
        self.assertEqual(len(res), 1)

        self.elastic_doc.remove(self.put_metadata(docc))
        res = self.elastic_conn.search(
            index="test.test",
            body={"query": {"match_all": {}}}
        )["hits"]["hits"]
        res = [x["_source"] for x in res]
        self.assertEqual(len(res), 0)

    def test_full_search(self):
        """Query ElasticSearch for all docs via API and via DocManager's
            _search(), compare.
        """

        docc = {'_id': '1', 'name': 'John'}
        self.elastic_doc.upsert(self.put_metadata(docc))
        docc = {'_id': '2', 'name': 'Paul'}
        self.elastic_doc.upsert(self.put_metadata(docc))
        search = list(self._search())
        search2 = []
        es_cursor = self.elastic_conn.search(
            index="test.test",
            body={"query": {"match_all": {}}})["hits"]["hits"]
        for doc in es_cursor:
            source = doc['_source']
            source['_id'] = doc['_id']
            search2.append(source)
        self.assertEqual(len(search), len(search2))
        self.assertNotEqual(len(search), 0)
        self.assertTrue(all(x in search for x in search2))
        self.assertTrue(all(y in search2 for y in search))

    def test_search(self):
        """Query ElasticSearch for docs in a timestamp range.

        We use API and DocManager's search(start_ts,end_ts), and then compare.
        """

        docc = {'_id': '1', 'name': 'John', '_ts': 5767301236327972865,
                'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        docc2 = {'_id': '2', 'name': 'John Paul', '_ts': 5767301236327972866,
                 'ns': 'test.test'}
        self.elastic_doc.upsert(docc2)
        docc3 = {'_id': '3', 'name': 'Paul', '_ts': 5767301236327972870,
                 'ns': 'test.test'}
        self.elastic_doc.upsert(docc3)
        search = list(self.elastic_doc.search(5767301236327972865,
                                              5767301236327972866))
        self.assertEqual(len(search), 2)
        result_ids = [result.get("_id") for result in search]
        self.assertIn('1', result_ids)
        self.assertIn('2', result_ids)

    def test_elastic_commit(self):
        """Test that documents get properly added to ElasticSearch.
        """

        docc = {'_id': '3', 'name': 'Waldo'}
        docman = DocManager(elastic_pair)
        # test cases:
        # -1 = no autocommit
        # 0 = commit immediately
        # x > 0 = commit within x seconds
        for autocommit_interval in [None, 0, 1, 2]:
            docman.auto_commit_interval = autocommit_interval
            docman.upsert(self.put_metadata(docc))
            if autocommit_interval is None:
                docman.commit()
            else:
                # Allow just a little extra time
                time.sleep(autocommit_interval + 1)
            results = list(self._search())
            self.assertEqual(len(results), 1,
                             "should commit document with "
                             "auto_commit_interval = %s" % str(
                                 autocommit_interval))
            self.assertEqual(results[0]["name"], "Waldo")
            self._remove()
        docman.stop()

    def test_get_last_doc(self):
        """Insert documents, verify that get_last_doc() returns the one with
            the latest timestamp.
        """
        base = self.elastic_doc.get_last_doc()
        ts = base.get("_ts", 0) if base else 0
        docc = {'_id': '4', 'name': 'Hare', '_ts': ts+3, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        docc = {'_id': '5', 'name': 'Tortoise', '_ts': ts+2, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        docc = {'_id': '6', 'name': 'Mr T.', '_ts': ts+1, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)

        self.assertEqual(
            self.elastic_doc.elastic.count(index="test.test")['count'], 3)
        doc = self.elastic_doc.get_last_doc()
        self.assertEqual(doc['_id'], '4')

        docc = {'_id': '6', 'name': 'HareTwin', '_ts': ts+4, 'ns': 'test.test'}
        self.elastic_doc.upsert(docc)
        doc = self.elastic_doc.get_last_doc()
        self.assertEqual(doc['_id'], '6')
        self.assertEqual(
            self.elastic_doc.elastic.count(index="test.test")['count'], 3)

if __name__ == '__main__':
    unittest.main()
