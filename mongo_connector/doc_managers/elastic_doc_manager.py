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

"""Receives documents from the oplog worker threads and indexes them
    into the backend.

    This file is a document manager for the Elastic search engine, but the
    intent is that this file can be used as an example to add on different
    backends. To extend this to other systems, simply implement the exact
    same class and replace the method definitions with API calls for the
    desired backend.
    """
import itertools
import logging

from threading import Timer

import bson.json_util

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch.helpers import scan, streaming_bulk

from mongo_connector import errors
from mongo_connector.constants import (DEFAULT_COMMIT_INTERVAL,
                                       DEFAULT_MAX_BULK)
from mongo_connector.util import retry_until_ok
from mongo_connector.doc_managers import DocManagerBase, exception_wrapper
from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter


wrap_exceptions = exception_wrapper({
    es_exceptions.ConnectionError: errors.ConnectionFailed,
    es_exceptions.TransportError: errors.OperationFailed})


class DocManager(DocManagerBase):
    """The DocManager class creates a connection to the backend engine and
        adds/removes documents, and in the case of rollback, searches for them.

        The reason for storing id/doc pairs as opposed to doc's is so that
        multiple updates to the same doc reflect the most up to date version as
        opposed to multiple, slightly different versions of a doc.

        We are using elastic native fields for _id and ns, but we also store
        them as fields in the document, due to compatibility issues.
        """

    def __init__(self, url, auto_commit_interval=DEFAULT_COMMIT_INTERVAL,
                 unique_key='_id', chunk_size=DEFAULT_MAX_BULK,
                 meta_index_name="mongodb_meta", meta_type="mongodb_meta",
                 **kwargs):
        """ Establish a connection to Elastic
        """
        self.elastic = Elasticsearch(hosts=[url])
        self.auto_commit_interval = auto_commit_interval
        self.doc_type = 'string'  # default type is string, change if needed
        self.meta_index_name = meta_index_name
        self.meta_type = meta_type
        self.unique_key = unique_key
        self.chunk_size = chunk_size
        if self.auto_commit_interval not in [None, 0]:
            self.run_auto_commit()
        self._formatter = DefaultDocumentFormatter()

    def stop(self):
        """ Stops the instance
        """
        self.auto_commit_interval = None

    def apply_update(self, doc, update_spec):
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    @wrap_exceptions
    def update(self, doc, update_spec):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        document = self.elastic.get(index=doc['ns'],
                                    id=str(doc['_id']))
        updated = self.apply_update(document['_source'], update_spec)
        # _id is immutable in MongoDB, so won't have changed in update
        updated['_id'] = document['_id']
        # Add metadata fields back into updated, for the purposes of
        # calling upsert(). Need to do this until these become separate
        # arguments in 2.x
        updated['ns'] = doc['ns']
        updated['_ts'] = doc['_ts']
        self.upsert(updated)
        # upsert() strips metadata, so only _id + fields in _source still here
        return updated

    @wrap_exceptions
    def upsert(self, doc):
        """Update or insert a document into Elastic

        If you'd like to have different types of document in your database,
        you can store the doc type as a field in Mongo and set doc_type to
        that field. (e.g. doc_type = doc['_type'])

        """
        doc_type = self.doc_type
        index = doc.pop('ns')
        # No need to duplicate '_id' in source document
        doc_id = str(doc.pop("_id"))
        metadata = {
            "ns": index,
            "_ts": doc.pop("_ts"),
            "_id": doc_id
        }
        # Index the source document
        self.elastic.index(index=index, doc_type=doc_type,
                           body=self._formatter.format_document(doc), id=doc_id,
                           refresh=(self.auto_commit_interval == 0))
        # Index document metadata
        self.elastic.index(index=self.meta_index_name, doc_type=self.meta_type,
                           body=bson.json_util.dumps(metadata), id=doc_id,
                           refresh=(self.auto_commit_interval == 0))
        # Leave _id, since it's part of the original document
        doc['_id'] = doc_id

    @wrap_exceptions
    def bulk_upsert(self, docs):
        """Update or insert multiple documents into Elastic

        docs may be any iterable
        """
        docs_iter, docs_meta_iter = itertools.tee(docs)

        def meta_docs_to_upsert():
            doc = None
            for doc in docs_meta_iter:
                document_meta = {
                    "_index": self.meta_index_name,
                    "_type": self.meta_type,
                    "_id": doc['_id'],
                    "_source": {
                        "_ns": doc['ns'],
                        "ts": doc['_ts']
                    }
                }
                yield document_meta
            if not doc:
                raise errors.EmptyDocsError(
                    "Cannot upsert an empty sequence of "
                    "documents into Elastic Search")

        def docs_to_upsert():
            doc = None
            for doc in docs_iter:
                # Remove metadata and redundant _id
                index = doc.pop("ns")
                doc_id = str(doc.pop("_id"))
                timestamp = doc.pop("_ts")
                yield {
                    "_index": index,
                    "_type": self.doc_type,
                    "_id": doc_id,
                    "_source": self._formatter.format_document(doc)
                }
                # Put metadata back for use in meta_docs_to_upsert
                doc['_id'] = doc_id
                doc['ns'] = index,
                doc['_ts'] = timestamp
            if not doc:
                raise errors.EmptyDocsError(
                    "Cannot upsert an empty sequence of "
                    "documents into Elastic Search")
        try:
            kw = {}
            if self.chunk_size > 0:
                kw['chunk_size'] = self.chunk_size

            meta_responses = streaming_bulk(client=self.elastic,
                                            actions=meta_docs_to_upsert(),
                                            **kw)
            responses = streaming_bulk(client=self.elastic,
                                       actions=docs_to_upsert(),
                                       **kw)

            for ok, resp in itertools.chain(responses, meta_responses):
                if not ok:
                    logging.error(
                        "Could not bulk-upsert document "
                        "into ElasticSearch: %r" % resp)
            if self.auto_commit_interval == 0:
                self.commit()
        except errors.EmptyDocsError:
            # This can happen when mongo-connector starts up, there is no
            # config file, but nothing to dump
            pass

    @wrap_exceptions
    def remove(self, doc):
        """Removes documents from Elastic

        The input is a python dictionary that represents a mongo document.
        """
        self.elastic.delete(index=doc['ns'], doc_type=self.doc_type,
                            id=str(doc["_id"]),
                            refresh=(self.auto_commit_interval == 0))
        self.elastic.delete(index=self.meta_index_name, doc_type=self.meta_type,
                            id=str(doc["_id"]),
                            refresh=(self.auto_commit_interval == 0))

    @wrap_exceptions
    def _stream_search(self, *args, **kwargs):
        """Helper method for iterating over ES search results."""
        for hit in scan(self.elastic, query=kwargs.pop('body', None),
                        scroll='10m', **kwargs):
            hit['_source']['_id'] = hit['_id']
            yield hit['_source']

    def search(self, start_ts, end_ts):
        """Called to query Elastic for documents in a time range."""
        return self._stream_search(
            index=self.meta_index_name,
            body={
                "query": {
                    "filtered": {
                        "filter": {
                            "range": {
                                "_ts": {"gte": start_ts, "lte": end_ts}
                            }
                        }
                    }
                }
            })

    def commit(self):
        """This function is used to force a refresh/commit.
        """
        retry_until_ok(self.elastic.indices.refresh, index="")

    def run_auto_commit(self):
        """Periodically commits to the Elastic server.
        """
        self.elastic.indices.refresh()
        if self.auto_commit_interval not in [None, 0]:
            Timer(self.auto_commit_interval, self.run_auto_commit).start()

    @wrap_exceptions
    def get_last_doc(self):
        """Returns the last document stored in the Elastic engine.
        """
        try:
            result = self.elastic.search(
                index=self.meta_index_name,
                body={
                    "query": {"match_all": {}},
                    "sort": [{"_ts": "desc"}],
                },
                size=1
            )["hits"]["hits"]
            for r in result:
                r['_source']['_id'] = r['_id']
                return r['_source']
        except es_exceptions.RequestError:
            # no documents so ES returns 400 because of undefined _ts mapping
            return None
