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

"""Test oplog manager methods
"""

import os
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest
import re
try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.util import long_to_bson_ts
from tests.setup_cluster import (start_cluster,
                                 kill_all)
from tests.util import wait_for
from mongo_connector.oplog_manager import OplogThread

PORTS_ONE = {"PRIMARY":  "27117", "SECONDARY":  "27118", "ARBITER":  "27119",
             "CONFIG":  "27220", "MAIN":  "27217"}

AUTH_FILE = os.environ.get('AUTH_FILE', None)
AUTH_USERNAME = os.environ.get('AUTH_USERNAME', None)
HOSTNAME = os.environ.get('HOSTNAME', "localhost")
PORTS_ONE['MAIN'] = os.environ.get('MAIN_ADDR', "27217")
CONFIG = os.environ.get('CONFIG', "config.txt")
TEMP_CONFIG = os.environ.get('TEMP_CONFIG', "temp_config.txt")


class TestOplogManager(unittest.TestCase):
    """Defines all the testing methods, as well as a method that sets up the
        cluster
    """
    def runTest(self):
        """Runs all Tests
        """
        unittest.TestCase.__init__(self)

    @classmethod
    def setUpClass(cls):
        """ Initializes the cluster
        """
        os.system('rm %s; touch %s' % (CONFIG, CONFIG))
        cls.AUTH_KEY = None
        cls.flag = True
        if AUTH_FILE:
            # We want to get the credentials from this file
            try:
                key = (open(AUTH_FILE)).read()
                re.sub(r'\s', '', key)
                cls.AUTH_KEY = key
            except IOError:
                print('Could not parse authentication file!')
                cls.flag = False
                cls.err_msg = "Could not read key file!"

        if not start_cluster(key_file=AUTH_FILE):
            cls.flag = False
            cls.err_msg = "Shards cannot be added to mongos"

    @classmethod
    def tearDownClass(cls):
        """ Kills cluster instance
        """
        kill_all()

    def setUp(self):
        """ Fails if we were unable to start the cluster
        """
        if not self.flag:
            self.fail(self.err_msg)

    @classmethod
    def get_oplog_thread(cls):
        """ Set up connection with mongo. Returns oplog, the connection and
            oplog collection

            This function clears the oplog
        """
        is_sharded = True
        primary_conn = Connection(HOSTNAME, int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection(HOSTNAME, int(PORTS_ONE["SECONDARY"]))

        primary_conn['test']['test'].drop()
        mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])

        if PORTS_ONE["MAIN"] == PORTS_ONE["PRIMARY"]:
            mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])
            is_sharded = False
        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog

        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=1000000)
        namespace_set = ['test.test']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, is_sharded,
                            doc_manager, LockingDict(),
                            namespace_set, cls.AUTH_KEY, AUTH_USERNAME,
                            repl_set="demo-repl")

        return(oplog, primary_conn, oplog_coll)
    
    @classmethod    
    def get_new_oplog(cls):
        """ Set up connection with mongo. Returns oplog, the connection and
            oplog collection

            This function does not clear the oplog
        """
        is_sharded = True
        primary_conn = Connection(HOSTNAME, int(PORTS_ONE["PRIMARY"]))
        if primary_conn['admin'].command("isMaster")['ismaster'] is False:
            primary_conn = Connection(HOSTNAME, int(PORTS_ONE["SECONDARY"]))

        mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])
        if PORTS_ONE["MAIN"] == PORTS_ONE["PRIMARY"]:
            mongos_addr = "%s:%s" % (HOSTNAME, PORTS_ONE['MAIN'])
            is_sharded = False
        oplog_coll = primary_conn['local']['oplog.rs']

        namespace_set = ['test.test']
        doc_manager = DocManager()
        oplog = OplogThread(primary_conn, mongos_addr, oplog_coll, is_sharded,
                            doc_manager, LockingDict(),
                            namespace_set, cls.AUTH_KEY, AUTH_USERNAME,
                            repl_set="demo-repl")
        return(oplog, primary_conn, oplog.main_connection, oplog_coll)

    def test_retrieve_doc(self):
        """Test retrieve_doc in oplog_manager. Assertion failure if it doesn't
            pass
        """
        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()
        #testing for entry as none type
        entry = None
        self.assertTrue(test_oplog.retrieve_doc(entry) is None)

        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)

        primary_conn['test']['test'].insert({'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        target_entry = primary_conn['test']['test'].find_one()

        #testing for search after inserting a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry),
                         target_entry)

        primary_conn['test']['test'].update({'name': 'paulie'},
                                            {"$set":  {'name': 'paul'}})

        last_oplog_entry = next(oplog_cursor)
        target_entry = primary_conn['test']['test'].find_one()

        #testing for search after updating a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry),
                         target_entry)

        primary_conn['test']['test'].remove({'name': 'paul'})
        last_oplog_entry = next(oplog_cursor)

        #testing for search after deleting a document
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry), None)

        last_oplog_entry['o']['_id'] = 'badID'

        #testing for bad doc id as input
        self.assertEqual(test_oplog.retrieve_doc(last_oplog_entry), None)

        #test_oplog.join()

    def test_get_oplog_cursor(self):
        """Test get_oplog_cursor in oplog_manager. Assertion failure if
            it doesn't pass
        """
        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()

        #test None cursor
        self.assertEqual(test_oplog.get_oplog_cursor(None), None)

        #test with one document
        primary_conn['test']['test'].insert({'name': 'paulie'})
        timestamp = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.get_oplog_cursor(timestamp)
        self.assertEqual(cursor.count(), 1)

        #test with two documents, one after the timestamp
        primary_conn['test']['test'].insert({'name': 'paul'})
        cursor = test_oplog.get_oplog_cursor(timestamp)
        self.assertEqual(cursor.count(), 2)

        #test case where timestamp is not in oplog which implies we're too
        #behind
        oplog_coll = primary_conn['local']['oplog.rs']
        oplog_coll.drop()           # reset the oplog
        primary_conn['local'].create_collection('oplog.rs', capped=True,
                                                size=10000)

        primary_conn['test']['test'].insert({'name': 'pauline'})
        self.assertEqual(test_oplog.get_oplog_cursor(timestamp), None)
        #test_oplog.join()
        #need to add tests for 'except' part of get_oplog_cursor

    def test_get_last_oplog_timestamp(self):
        """Test get_last_oplog_timestamp in oplog_manager. Assertion failure
            if it doesn't pass
        """

        #test empty oplog
        test_oplog, primary_conn, oplog_coll = self.get_oplog_thread()
        self.assertEqual(test_oplog.get_last_oplog_timestamp(), None)

        #test non-empty oplog
        oplog_cursor = oplog_coll.find({}, tailable=True, await_data=True)
        primary_conn['test']['test'].insert({'name': 'paulie'})
        last_oplog_entry = next(oplog_cursor)
        self.assertEqual(test_oplog.get_last_oplog_timestamp(),
                         last_oplog_entry['ts'])

        #test_oplog.join()

    def test_dump_collection(self):
        """Test dump_collection in oplog_manager. Assertion failure if it
            doesn't pass
        """

        test_oplog, primary_conn, search_ts = self.get_oplog_thread()

        #with documents
        primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        test_oplog.dump_collection()

        doc_manager = test_oplog.doc_managers[0]
        doc_manager.commit()
        solr_results = doc_manager._search()
        self.assertEqual(len(solr_results), 1)
        solr_doc = solr_results[0]
        self.assertEqual(long_to_bson_ts(solr_doc['_ts']), search_ts)
        self.assertEqual(solr_doc['name'], 'paulie')
        self.assertEqual(solr_doc['ns'], 'test.test')

        # test multiple targets
        doc_managers = [DocManager(), DocManager(), DocManager()]
        test_oplog.doc_managers = doc_managers
        primary_conn["test"]["test"].remove()
        for i in range(1000):
            primary_conn["test"]["test"].insert({"i": i})
        test_oplog.dump_collection()
        for dm in doc_managers:
            self.assertEqual(len(dm._search()), 1000)

    def test_init_cursor(self):
        """Test init_cursor in oplog_manager. Assertion failure if it
            doesn't pass
        """

        test_oplog, primary_conn, search_ts = self.get_oplog_thread()
        test_oplog.checkpoint = None  # needed for these tests

        # initial tests with no config file and empty oplog
        self.assertEqual(test_oplog.init_cursor(), None)

        # no config, single oplog entry
        primary_conn['test']['test'].insert({'name': 'paulie'})
        search_ts = test_oplog.get_last_oplog_timestamp()
        cursor = test_oplog.init_cursor()
        self.assertEqual(cursor.count(), 1)
        self.assertEqual(test_oplog.checkpoint, search_ts)

        # with config file, assert that size != 0
        os.system('touch temp_config.txt')

        cursor = test_oplog.init_cursor()
        oplog_dict = test_oplog.oplog_progress.get_dict()

        self.assertEqual(cursor.count(), 1)
        self.assertTrue(str(test_oplog.oplog) in oplog_dict)
        self.assertTrue(oplog_dict[str(test_oplog.oplog)] ==
                        test_oplog.checkpoint)

        os.system('rm temp_config.txt')

        # test init_cursor when OplogThread created with/without no-dump option
        # insert some documents (will need to be dumped)
        primary_conn['test']['test'].remove()
        primary_conn['test']['test'].insert(({"_id": i} for i in range(100)))

        # test no-dump option
        docman = test_oplog.doc_managers[0]
        docman._delete()
        test_oplog.collection_dump = False
        test_oplog.oplog_progress = LockingDict()
        # init_cursor has the side-effect of causing a collection dump
        test_oplog.init_cursor()
        self.assertEqual(len(docman._search()), 0)

        # test w/o no-dump option
        docman._delete()
        test_oplog.collection_dump = True
        test_oplog.oplog_progress = LockingDict()
        test_oplog.init_cursor()
        self.assertEqual(len(docman._search()), 100)

    def test_filter_fields(self):
        opman, _, _ = self.get_oplog_thread()
        docman = opman.doc_managers[0]
        conn = opman.main_connection

        include_fields = ["a", "b", "c"]
        exclude_fields = ["d", "e", "f"]

        # Set fields to care about
        opman.fields = include_fields
        # Documents have more than just these fields
        doc = {
            "a": 1, "b": 2, "c": 3,
            "d": 4, "e": 5, "f": 6,
            "_id": 1
        }
        db = conn['test']['test']
        db.insert(doc)
        wait_for(lambda: db.count() == 1)
        opman.dump_collection()

        result = docman._search()[0]
        keys = result.keys()
        for inc, exc in zip(include_fields, exclude_fields):
            self.assertIn(inc, keys)
            self.assertNotIn(exc, keys)

    def test_many_targets(self):
        """Test that one OplogThread is capable of replicating to more than
        one target.
        """

        opman, primary_conn, oplog_coll = self.get_oplog_thread()
        doc_managers = [DocManager(), DocManager(), DocManager()]
        opman.doc_managers = doc_managers

        # start replicating
        opman.start()
        primary_conn["test"]["test"].insert({
            "name": "kermit",
            "color": "green"
        })
        primary_conn["test"]["test"].insert({
            "name": "elmo",
            "color": "firetruck red"
        })

        self.assertTrue(
            wait_for(lambda: sum(len(d._search()) for d in doc_managers) == 6),
            "OplogThread should be able to replicate to multiple targets"
        )

        primary_conn["test"]["test"].remove({"name": "elmo"})

        self.assertTrue(
            wait_for(lambda: sum(len(d._search()) for d in doc_managers) == 3),
            "OplogThread should be able to replicate to multiple targets"
        )
        for d in doc_managers:
            self.assertEqual(d._search()[0]["name"], "kermit")

        # cleanup
        opman.join()

if __name__ == '__main__':
    unittest.main()
