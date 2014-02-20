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

import time
import os
import shutil
import sys
if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

import bson
import pymongo
from pymongo.read_preferences import ReadPreference
try:
    from pymongo import MongoClient as Connection
except ImportError:
    from pymongo import Connection

from mongo_connector.doc_managers.doc_manager_simulator import DocManager
from mongo_connector.locking_dict import LockingDict
from mongo_connector.oplog_manager import OplogThread
from mongo_connector.util import (bson_ts_to_long,
                                  retry_until_ok)
from tests.setup_cluster import (
    kill_mongo_proc,
    start_mongo_proc,
    start_cluster,
    kill_all,
    DEMO_SERVER_DATA,
    PORTS_ONE,
    PORTS_TWO
)
from tests.util import wait_for


class TestOplogManagerSharded(unittest.TestCase):
    """Defines all test cases for OplogThreads running on a sharded
    cluster
    """

    @classmethod
    def setUpClass(cls):
        """ Initialize the cluster:

        Clean out the databases used by the tests
        Make connections to mongos, mongods
        Create and shard test collections
        Create OplogThreads
        """
        # Create a new oplog progress file
        try:
            os.unlink("config.txt")
        except OSError:
            pass
        open("config.txt", "w").close()

        # Start the cluster with a mongos on port 27217
        start_cluster(sharded=True)

        # Connection to mongos
        mongos_address = "localhost:%s" % PORTS_ONE["MONGOS"]
        cls.mongos_conn = Connection(mongos_address)

        # Connections to the shards
        cls.shard1_conn = Connection("localhost:%s" % PORTS_ONE["PRIMARY"],
                                     replicaSet="demo-repl")
        cls.shard2_conn = Connection("localhost:%s" % PORTS_TWO["PRIMARY"],
                                     replicaSet="demo-repl-2")
        cls.shard1_secondary_conn = Connection(
            "localhost:%s" % PORTS_ONE["SECONDARY"],
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )
        cls.shard2_secondary_conn = Connection(
            "localhost:%s" % PORTS_TWO["SECONDARY"],
            read_preference=ReadPreference.SECONDARY_PREFERRED
        )

        # Wipe any test data
        cls.mongos_conn["test"]["mcsharded"].drop()

        # Create and shard the collection test.mcsharded on the "i" field
        cls.mongos_conn["test"]["mcsharded"].ensure_index("i")
        cls.mongos_conn.admin.command("enableSharding", "test")
        cls.mongos_conn.admin.command("shardCollection",
                                      "test.mcsharded",
                                      key={"i": 1})
        # Pre-split the collection so that:
        # i < 1000            lives on shard1
        # i >= 1000           lives on shard2
        cls.mongos_conn.admin.command(bson.SON([
            ("split", "test.mcsharded"),
            ("middle", {"i": 1000})
        ]))
        # Tag shards/ranges to enforce chunk split rules
        cls.mongos_conn.config.shards.update(
            {"_id": "demo-repl"},
            {"$addToSet": {"tags": "small-i"}}
        )
        cls.mongos_conn.config.shards.update(
            {"_id": "demo-repl-2"},
            {"$addToSet": {"tags": "large-i"}}
        )
        cls.mongos_conn.config.tags.update(
            {"_id": bson.son.SON([("ns", "test.mcsharded"),
                                  ("min", {"i": bson.min_key.MinKey()})])},
            bson.son.SON([
                ("_id", bson.son.SON([
                        ("ns", "test.mcsharded"),
                        ("min", {"i": bson.min_key.MinKey()})
                    ])),
                ("ns", "test.mcsharded"),
                ("min", {"i": bson.min_key.MinKey()}),
                ("max", {"i": 1000}),
                ("tag", "small-i")
            ]),
            upsert=True
        )
        cls.mongos_conn.config.tags.update(
            {"_id": bson.son.SON([("ns", "test.mcsharded"),
                                  ("min", {"i": 1000})])},
            bson.son.SON([
                ("_id", bson.son.SON([
                        ("ns", "test.mcsharded"),
                        ("min", {"i": 1000})
                    ])),
                ("ns", "test.mcsharded"),
                ("min", {"i": 1000}),
                ("max", {"i": bson.max_key.MaxKey()}),
                ("tag", "large-i")
            ]),
            upsert=True
        )
        # Move chunks to their proper places
        try:
            cls.mongos_conn["admin"].command(
                bson.son.SON([
                    ("moveChunk", "test.mcsharded"),
                    ("find", {"i": 1}),
                    ("to", "demo-repl")
                ])
            )
        except pymongo.errors.OperationFailure:
            pass        # chunk may already be on the correct shard
        try:
            cls.mongos_conn["admin"].command(
                bson.son.SON([
                    ("moveChunk", "test.mcsharded"),
                    ("find", {"i": 1000}),
                    ("to", "demo-repl-2")
                ])
            )
        except pymongo.errors.OperationFailure:
            pass        # chunk may already be on the correct shard

        # Wait for things to settle down
        cls.mongos_conn["test"]["mcsharded"].insert({"i": 1})
        cls.mongos_conn["test"]["mcsharded"].insert({"i": 1000})

        def chunks_moved():
            shard1_done = cls.shard1_conn.test.mcsharded.find_one() is not None
            shard2_done = cls.shard2_conn.test.mcsharded.find_one() is not None
            return shard1_done and shard2_done
        assert(wait_for(chunks_moved))
        cls.mongos_conn.test.mcsharded.remove()

        # Oplog threads (oplog manager) for each shard
        doc_manager = DocManager()
        oplog_progress = LockingDict()
        cls.opman1 = OplogThread(
            primary_conn=cls.shard1_conn,
            main_address=mongos_address,
            oplog_coll=cls.shard1_conn["local"]["oplog.rs"],
            is_sharded=True,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mcsharded", "test.mcunsharded"],
            auth_key=None,
            auth_username=None
        )
        cls.opman2 = OplogThread(
            primary_conn=cls.shard2_conn,
            main_address=mongos_address,
            oplog_coll=cls.shard2_conn["local"]["oplog.rs"],
            is_sharded=True,
            doc_manager=doc_manager,
            oplog_progress_dict=oplog_progress,
            namespace_set=["test.mcsharded", "test.mcunsharded"],
            auth_key=None,
            auth_username=None
        )

    @classmethod
    def tearDownClass(cls):
        """ Kill the cluster
        """
        # kill_all()

    def setUp(self):
        # clear oplog
        self.shard1_conn["local"]["oplog.rs"].drop()
        self.shard2_conn["local"]["oplog.rs"].drop()
        self.shard1_conn["local"].create_collection(
            "oplog.rs",
            size=1024 * 1024 * 100,       # 100MB
            capped=True
        )
        self.shard2_conn["local"].create_collection(
            "oplog.rs",
            size=1024 * 1024 * 100,       # 100MB
            capped=True
        )
        # re-sync secondaries
        try:
            self.shard1_secondary_conn["admin"].command("shutdown")
        except pymongo.errors.AutoReconnect:
            pass
        try:
            self.shard2_secondary_conn["admin"].command("shutdown")
        except pymongo.errors.AutoReconnect:
            pass
        data1 = os.path.join(DEMO_SERVER_DATA, "replset1b")
        data2 = os.path.join(DEMO_SERVER_DATA, "replset2b")
        shutil.rmtree(data1)
        shutil.rmtree(data2)
        os.mkdir(data1)
        os.mkdir(data2)
        conf1 = self.shard1_conn["local"]["system.replset"].find_one()
        conf2 = self.shard2_conn["local"]["system.replset"].find_one()
        conf1["version"] += 1
        conf2["version"] += 1
        self.shard1_conn["admin"].command({"replSetReconfig": conf1})
        self.shard2_conn["admin"].command({"replSetReconfig": conf2})
        start_mongo_proc(PORTS_ONE["SECONDARY"], "demo-repl", "replset1b",
                         "replset1b.log", None)
        start_mongo_proc(PORTS_TWO["SECONDARY"], "demo-repl-2", "replset2b",
                         "replset2b.log", None)

        def secondary_up(connection):
            def wrap():
                return retry_until_ok(
                    connection["admin"].command,
                    "replSetGetStatus"
                )["myState"] == 2
            return wrap
        wait_for(secondary_up(self.shard1_secondary_conn))
        wait_for(secondary_up(self.shard2_secondary_conn))

    def tearDown(self):
        self.mongos_conn["test"]["mcsharded"].remove()
        self.mongos_conn["test"]["mcunsharded"].remove()

    def test_retrieve_doc(self):
        """ Test the retrieve_doc method """

        # Trivial case where the oplog entry is None
        self.assertEqual(self.opman1.retrieve_doc(None), None)

        # Retrieve a document from insert operation in oplog
        doc = {"name": "mango", "type": "fruit",
               "ns": "test.mcsharded", "weight": 3.24, "i": 1}
        self.mongos_conn["test"]["mcsharded"].insert(doc)
        oplog_entries = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.DESCENDING)],
            limit=1
        )
        oplog_entry = next(oplog_entries)
        self.assertEqual(self.opman1.retrieve_doc(oplog_entry), doc)

        # Retrieve a document from update operation in oplog
        self.mongos_conn["test"]["mcsharded"].update(
            {"i": 1},
            {"$set": {"sounds-like": "mongo"}}
        )
        oplog_entries = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.DESCENDING)],
            limit=1
        )
        doc["sounds-like"] = "mongo"
        self.assertEqual(self.opman1.retrieve_doc(next(oplog_entries)), doc)

        # Retrieve a document from remove operation in oplog
        # (expected: None)
        self.mongos_conn["test"]["mcsharded"].remove({
            "i": 1
        })
        oplog_entries = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.DESCENDING)],
            limit=1
        )
        self.assertEqual(self.opman1.retrieve_doc(next(oplog_entries)), None)

        # Retrieve a document with bad _id
        # (expected: None)
        oplog_entry["o"]["_id"] = "ThisIsNotAnId123456789"
        self.assertEqual(self.opman1.retrieve_doc(oplog_entry), None)

    def test_get_oplog_cursor(self):
        """Test the get_oplog_cursor method"""

        # Trivial case: timestamp is None
        self.assertEqual(self.opman1.get_oplog_cursor(None), None)

        # earliest entry is after given timestamp
        doc = {"ts": bson.Timestamp(1000, 0), "i": 1}
        self.mongos_conn["test"]["mcsharded"].insert(doc)
        self.assertEqual(self.opman1.get_oplog_cursor(
            bson.Timestamp(1, 0)), None)

        # earliest entry is the only one at/after timestamp
        latest_timestamp = self.opman1.get_last_oplog_timestamp()
        cursor = self.opman1.get_oplog_cursor(latest_timestamp)
        self.assertNotEqual(cursor, None)
        self.assertEqual(cursor.count(), 1)
        self.assertEqual(self.opman1.retrieve_doc(cursor[0]), doc)

        # many entries before and after timestamp
        for i in range(2, 2002):
            self.mongos_conn["test"]["mcsharded"].insert({
                "i": i
            })
        oplog1 = self.shard1_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.ASCENDING)]
        )
        oplog2 = self.shard2_conn["local"]["oplog.rs"].find(
            sort=[("ts", pymongo.ASCENDING)]
        )

        # startup message + insert at beginning of tests + many inserts
        self.assertEqual(oplog1.count(), 1 + 1 + 998)
        self.assertEqual(oplog2.count(), 1 + 1002)
        pivot1 = oplog1.skip(400).limit(1)[0]
        pivot2 = oplog2.skip(400).limit(1)[0]

        cursor1 = self.opman1.get_oplog_cursor(pivot1["ts"])
        cursor2 = self.opman2.get_oplog_cursor(pivot2["ts"])
        self.assertEqual(cursor1.count(), 1 + 1 + 998 - 400)
        self.assertEqual(cursor2.count(), 1 + 1002 - 400)

        # get_oplog_cursor fast-forwards *one doc beyond* the given timestamp
        doc1 = self.mongos_conn["test"]["mcsharded"].find_one(
            {"_id": next(cursor1)["o"]["_id"]})
        doc2 = self.mongos_conn["test"]["mcsharded"].find_one(
            {"_id": next(cursor2)["o"]["_id"]})
        self.assertEqual(doc1["i"], self.opman1.retrieve_doc(pivot1)["i"] + 1)
        self.assertEqual(doc2["i"], self.opman2.retrieve_doc(pivot2)["i"] + 1)

    def test_get_last_oplog_timestamp(self):
        """Test the get_last_oplog_timestamp method"""

        # "empty" the oplog
        self.opman1.oplog = self.shard1_conn["test"]["emptycollection"]
        self.opman2.oplog = self.shard2_conn["test"]["emptycollection"]
        self.assertEqual(self.opman1.get_last_oplog_timestamp(), None)
        self.assertEqual(self.opman2.get_last_oplog_timestamp(), None)

        # Test non-empty oplog
        self.opman1.oplog = self.shard1_conn["local"]["oplog.rs"]
        self.opman2.oplog = self.shard2_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.mongos_conn["test"]["mcsharded"].insert({
                "i": i + 500
            })
        oplog1 = self.shard1_conn["local"]["oplog.rs"]
        oplog1 = oplog1.find().sort("$natural", pymongo.DESCENDING).limit(1)[0]
        oplog2 = self.shard2_conn["local"]["oplog.rs"]
        oplog2 = oplog2.find().sort("$natural", pymongo.DESCENDING).limit(1)[0]
        self.assertEqual(self.opman1.get_last_oplog_timestamp(),
                         oplog1["ts"])
        self.assertEqual(self.opman2.get_last_oplog_timestamp(),
                         oplog2["ts"])

    def test_dump_collection(self):
        """Test the dump_collection method

        Cases:

        1. empty oplog
        2. non-empty oplog
        """

        # Test with empty oplog
        self.opman1.oplog = self.shard1_conn["test"]["emptycollection"]
        self.opman2.oplog = self.shard2_conn["test"]["emptycollection"]
        last_ts1 = self.opman1.dump_collection()
        last_ts2 = self.opman2.dump_collection()
        self.assertEqual(last_ts1, None)
        self.assertEqual(last_ts2, None)

        # Test with non-empty oplog
        self.opman1.oplog = self.shard1_conn["local"]["oplog.rs"]
        self.opman2.oplog = self.shard1_conn["local"]["oplog.rs"]
        for i in range(1000):
            self.mongos_conn["test"]["mcsharded"].insert({
                "i": i + 500
            })
        last_ts1 = self.opman1.get_last_oplog_timestamp()
        last_ts2 = self.opman2.get_last_oplog_timestamp()
        self.assertEqual(last_ts1, self.opman1.dump_collection())
        self.assertEqual(last_ts2, self.opman2.dump_collection())
        self.assertEqual(len(self.opman1.doc_manager._search()),
                         len(self.opman2.doc_manager._search()))
        self.assertEqual(len(self.opman1.doc_manager._search()), 1000)

    def test_init_cursor(self):
        """Test the init_cursor method

        Cases:

        1. no last checkpoint, no collection dump
        2. no last checkpoint, collection dump ok and stuff to dump
        3. no last checkpoint, nothing to dump, stuff in oplog
        4. no last checkpoint, nothing to dump, nothing in oplog
        5. last checkpoint exists
        """

        # N.B. these sub-cases build off of each other and cannot be re-ordered
        # without side-effects

        # No last checkpoint, no collection dump, nothing in oplog
        # "change oplog collection" to put nothing in oplog
        self.opman1.oplog = self.shard1_conn["test"]["emptycollection"]
        self.opman2.oplog = self.shard2_conn["test"]["emptycollection"]
        self.opman1.collection_dump = False
        self.opman2.collection_dump = False
        self.assertEqual(self.opman1.init_cursor(), None)
        self.assertEqual(self.opman1.checkpoint, None)
        self.assertEqual(self.opman2.init_cursor(), None)
        self.assertEqual(self.opman2.checkpoint, None)

        # No last checkpoint, empty collections, nothing in oplog
        self.opman1.collection_dump = True
        self.opman2.collection_dump = True
        self.assertEqual(self.opman1.init_cursor(), None)
        self.assertEqual(self.opman1.checkpoint, None)
        self.assertEqual(self.opman2.init_cursor(), None)
        self.assertEqual(self.opman2.checkpoint, None)

        # No last checkpoint, empty collections, something in oplog
        self.opman1.oplog = self.shard1_conn["local"]["oplog.rs"]
        self.opman2.oplog = self.shard2_conn["local"]["oplog.rs"]
        oplog_startup_ts = self.opman2.get_last_oplog_timestamp()
        collection = self.mongos_conn["test"]["mcsharded"]
        collection.insert({"i": 1})
        collection.remove({"i": 1})
        time.sleep(3)
        last_ts1 = self.opman1.get_last_oplog_timestamp()
        self.assertEqual(next(self.opman1.init_cursor())["ts"], last_ts1)
        self.assertEqual(self.opman1.checkpoint, last_ts1)
        with self.opman1.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman1.oplog)], last_ts1)
        # init_cursor should point to startup message in shard2 oplog
        cursor = self.opman2.init_cursor()
        self.assertEqual(next(cursor)["ts"], oplog_startup_ts)
        self.assertEqual(self.opman2.checkpoint, oplog_startup_ts)

        # No last checkpoint, non-empty collections, stuff in oplog
        progress = LockingDict()
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        collection.insert({"i": 1200})
        last_ts2 = self.opman2.get_last_oplog_timestamp()
        self.assertEqual(next(self.opman1.init_cursor())["ts"], last_ts1)
        self.assertEqual(self.opman1.checkpoint, last_ts1)
        with self.opman1.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman1.oplog)], last_ts1)
        self.assertEqual(next(self.opman2.init_cursor())["ts"], last_ts2)
        self.assertEqual(self.opman2.checkpoint, last_ts2)
        with self.opman2.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman2.oplog)], last_ts2)

        # Last checkpoint exists
        progress = LockingDict()
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        for i in range(1000):
            collection.insert({"i": i + 500})
        entry1 = list(
            self.shard1_conn["local"]["oplog.rs"].find(skip=200, limit=2))
        entry2 = list(
            self.shard2_conn["local"]["oplog.rs"].find(skip=200, limit=2))
        progress.get_dict()[str(self.opman1.oplog)] = entry1[0]["ts"]
        progress.get_dict()[str(self.opman2.oplog)] = entry2[0]["ts"]
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        self.opman1.checkpoint = self.opman2.checkpoint = None
        cursor1 = self.opman1.init_cursor()
        cursor2 = self.opman2.init_cursor()
        self.assertEqual(entry1[1]["ts"], next(cursor1)["ts"])
        self.assertEqual(entry2[1]["ts"], next(cursor2)["ts"])
        self.assertEqual(self.opman1.checkpoint, entry1[0]["ts"])
        self.assertEqual(self.opman2.checkpoint, entry2[0]["ts"])
        with self.opman1.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman1.oplog)],
                             entry1[0]["ts"])
        with self.opman2.oplog_progress as prog:
            self.assertEqual(prog.get_dict()[str(self.opman2.oplog)],
                             entry2[0]["ts"])

        # Some cleanup
        progress = LockingDict()
        self.opman1.oplog_progress = self.opman2.oplog_progress = progress
        self.opman1.checkpoint = self.opman2.checkpoint = None

    def test_rollback(self):
        """Test the rollback method in a sharded environment

        Cases:
        1. Documents on both shards, rollback on one shard
        2. Documents on both shards, rollback on both shards

        """

        self.opman1.start()
        self.opman2.start()

        # Insert first documents while primaries are up
        db_main = self.mongos_conn["test"]["mcsharded"]
        db_main.insert({"i": 0})
        db_main.insert({"i": 1000})
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].count(), 1)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].count(), 1)

        # Make sure insert is replicated
        db_secondary1 = self.shard1_secondary_conn["test"]["mcsharded"]
        db_secondary2 = self.shard2_secondary_conn["test"]["mcsharded"]
        self.assertTrue(wait_for(lambda: db_secondary1.count() == 1),
                        "write to shard1 didn't replicate")
        self.assertTrue(wait_for(lambda: db_secondary2.count() == 1),
                        "write to shard2 didn't replicate")

        # Case 1: only one primary goes down, shard1 in this case
        kill_mongo_proc("localhost", PORTS_ONE["PRIMARY"])

        # Wait for the secondary to be promoted
        shard1_secondary_admin = self.shard1_secondary_conn["admin"]
        while not shard1_secondary_admin.command("isMaster")["ismaster"]:
            time.sleep(1)

        # Insert another document. This will be rolled back later
        retry_until_ok(db_main.insert, {"i": 1})
        self.assertEqual(db_secondary1.count(), 2)

        # Wait for replication on the doc manager
        # Note that both OplogThreads share the same doc manager
        c = lambda: len(self.opman1.doc_manager._search()) == 3
        self.assertTrue(wait_for(c),
                        "not all writes were replicated to doc manager")

        # Kill the new primary
        kill_mongo_proc("localhost", PORTS_ONE["SECONDARY"])

        # Start both servers back up
        start_mongo_proc(
            port=PORTS_ONE["PRIMARY"],
            repl_set_name="demo-repl",
            data="replset1a",
            log="replset1a.log",
            key_file=None
        )
        primary_admin = self.shard1_conn["admin"]
        c = lambda: primary_admin.command("isMaster")["ismaster"]
        while not retry_until_ok(c):
            time.sleep(1)
        start_mongo_proc(
            port=PORTS_ONE["SECONDARY"],
            repl_set_name="demo-repl",
            data="replset1b",
            log="replset1b.log",
            key_file=None
        )
        secondary_admin = self.shard1_secondary_conn["admin"]
        while not secondary_admin.command("replSetGetStatus")["myState"] != 2:
            time.sleep(1)
        query = {"i": {"$lt": 1000}}
        while retry_until_ok(lambda: db_main.find(query).count) == 0:
            time.sleep(1)

        # Only first document should exist in MongoDB
        self.assertEqual(db_main.find(query).count(), 1)
        self.assertEqual(db_main.find_one(query)["i"], 0)

        # Same should hold for the doc manager
        docman_docs = [d for d in self.opman1.doc_manager._search()
                       if d["i"] < 1000]
        self.assertEqual(len(docman_docs), 1)
        self.assertEqual(docman_docs[0]["i"], 0)

        # Wait for previous rollback to complete
        def rollback_done():
            secondary1_count = retry_until_ok(db_secondary1.count)
            secondary2_count = retry_until_ok(db_secondary2.count)
            return (1, 1) == (secondary1_count, secondary2_count)
        self.assertTrue(wait_for(rollback_done),
                        "rollback never replicated to one or more secondaries")

        ##############################

        # Case 2: Primaries on both shards go down
        kill_mongo_proc("localhost", PORTS_ONE["PRIMARY"])
        kill_mongo_proc("localhost", PORTS_TWO["PRIMARY"])

        # Wait for the secondaries to be promoted
        shard1_secondary_admin = self.shard1_secondary_conn["admin"]
        shard2_secondary_admin = self.shard2_secondary_conn["admin"]
        while not shard1_secondary_admin.command("isMaster")["ismaster"]:
            time.sleep(1)
        while not shard2_secondary_admin.command("isMaster")["ismaster"]:
            time.sleep(1)

        # Insert another document on each shard. These will be rolled back later
        retry_until_ok(db_main.insert, {"i": 1})
        self.assertEqual(db_secondary1.count(), 2)
        retry_until_ok(db_main.insert, {"i": 1001})
        self.assertEqual(db_secondary2.count(), 2)

        # Wait for replication on the doc manager
        c = lambda: len(self.opman1.doc_manager._search()) == 4
        self.assertTrue(wait_for(c),
                        "not all writes were replicated to doc manager")

        # Kill the new primaries
        kill_mongo_proc("localhost", PORTS_ONE["SECONDARY"])
        kill_mongo_proc("localhost", PORTS_TWO["SECONDARY"])

        # Start the servers back up...
        # Shard 1
        start_mongo_proc(
            port=PORTS_ONE["PRIMARY"],
            repl_set_name="demo-repl",
            data="replset1a",
            log="replset1a.log",
            key_file=None
        )
        c = lambda: self.shard1_conn['admin'].command("isMaster")["ismaster"]
        while not retry_until_ok(c):
            time.sleep(1)
        start_mongo_proc(
            port=PORTS_ONE["SECONDARY"],
            repl_set_name="demo-repl",
            data="replset1b",
            log="replset1b.log",
            key_file=None
        )
        secondary_admin = self.shard1_secondary_conn["admin"]
        while not secondary_admin.command("replSetGetStatus")["myState"] != 2:
            time.sleep(1)
        while retry_until_ok(lambda: db_main.find(query).count) == 0:
            time.sleep(1)
        # Shard 2
        start_mongo_proc(
            port=PORTS_TWO["PRIMARY"],
            repl_set_name="demo-repl-2",
            data="replset2a",
            log="replset2a.log",
            key_file=None
        )
        c = lambda: self.shard2_conn['admin'].command("isMaster")["ismaster"]
        while not retry_until_ok(c):
            time.sleep(1)
        start_mongo_proc(
            port=PORTS_TWO["SECONDARY"],
            repl_set_name="demo-repl-2",
            data="replset2b",
            log="replset2b.log",
            key_file=None
        )
        secondary_admin = self.shard2_secondary_conn["admin"]
        while not secondary_admin.command("replSetGetStatus")["myState"] != 2:
            time.sleep(1)
        query2 = {"i": {"$gte": 1000}}
        while retry_until_ok(lambda: db_main.find(query).count) == 0:
            time.sleep(1)

        # Only first documents should exist in MongoDB
        self.assertEqual(db_main.find(query).count(), 1)
        self.assertEqual(db_main.find_one(query)["i"], 0)
        self.assertEqual(db_main.find(query2).count(), 1)
        self.assertEqual(db_main.find_one(query2)["i"], 1000)

        # Same should hold for the doc manager
        i_values = [d["i"] for d in self.opman1.doc_manager._search()]
        self.assertEqual(len(i_values), 2)
        self.assertIn(0, i_values)
        self.assertIn(1000, i_values)

        # cleanup
        self.opman1.join()
        self.opman2.join()

    # TODO: Code review for test_rollback before committing this! --v
    def test_with_chunk_migration(self):
        """Test that DocManagers have proper state after both a successful
        and an unsuccessful chunk migration
        """

        # Start replicating to dummy doc managers
        self.opman1.start()
        self.opman2.start()

        collection = self.mongos_conn["test"]["mcsharded"]
        for i in range(1000):
            collection.insert({"i": i + 500})
        # Assert current state of the mongoverse
        self.assertEqual(self.shard1_conn["test"]["mcsharded"].find().count(),
                         500)
        self.assertEqual(self.shard2_conn["test"]["mcsharded"].find().count(),
                         500)
        wait_for(lambda: len(self.opman1.doc_manager._search()) == 1000)
        self.assertEqual(len(self.opman1.doc_manager._search()), 1000)

        # Test successful chunk move from shard 1 to shard 2
        self.mongos_conn["admin"].command(
            bson.son.SON([
                ("moveChunk", "test.mcsharded"),
                ("find", {"i": 1}),
                ("to", "demo-repl-2")
            ])
        )

        # Wait for the balancer to kick-in
        def balancer_running():
            balancer_lock = self.mongos_conn["config"]["locks"].find_one(
                {"_id": "balancer"})
            return balancer_lock["state"] > 0 if balancer_lock else False
        wait_for(balancer_running)
        # doc manager should have all docs
        all_docs = self.opman1.doc_manager._search()
        self.assertEqual(len(all_docs), 1000)
        for i, doc in enumerate(sorted(all_docs, key=lambda x: x["i"])):
            self.assertEqual(doc["i"], i + 500)

        # Wait for migration to finish
        def migration_done():
            chunks = self.mongos_conn["config"]["chunks"]
            return chunks.find({"shard": "replset-test-2"}).count() == 2
        wait_for(migration_done)
        # doc manager should still have all docs
        all_docs = self.opman1.doc_manager._search()
        self.assertEqual(len(all_docs), 1000)
        for i, doc in enumerate(sorted(all_docs, key=lambda x: x["i"])):
            self.assertEqual(doc["i"], i + 500)

        # Mark the collection as "dropped". This will cause migration to fail.
        self.mongos_conn["config"]["collections"].update(
            {"_id": "test.mcsharded"},
            {"$set": {"dropped": True}}
        )

        # Test unsuccessful chunk move from shard 2 to shard 1
        def fail_to_move_chunk():
            self.mongos_conn["admin"].command(
                bson.son.SON([
                    ("moveChunk", "test.mcsharded"),
                    ("find", {"i": 1}),
                    ("to", "demo-repl")
                ])
            )
        self.assertRaises(pymongo.errors.OperationFailure, fail_to_move_chunk)
        # doc manager should still have all docs
        all_docs = self.opman1.doc_manager._search()
        self.assertEqual(len(all_docs), 1000)
        for i, doc in enumerate(sorted(all_docs, key=lambda x: x["i"])):
            self.assertEqual(doc["i"], i + 500)

        # Cleanup
        self.opman1.join()
        self.opman2.join()
        progress = LockingDict()
        self.opman1.oplog_progress = progress
        self.opman2.oplog_progress = progress
        self.opman1.checkpoint = self.opman2.checkpoint = None

if __name__ == '__main__':
    unittest.main()
