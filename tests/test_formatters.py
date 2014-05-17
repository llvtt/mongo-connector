import re
import sys
import uuid

if sys.version_info[:2] == (2, 6):
    import unittest2 as unittest
else:
    import unittest

import bson

from mongo_connector.doc_managers.formatters import (
    DefaultDocumentFormatter, DocumentFlattener)


class TestFormatters(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Some test values to use
        cls.bin1 = bson.Binary(b"\x00hello\x00", 0)
        cls.bin2 = b'\x00hello\x00'
        cls.xuuid = uuid.uuid4()
        cls.oid = bson.ObjectId()
        cls.regex = re.compile("hello", re.VERBOSE | re.MULTILINE)
        cls.doc = {'r': cls.regex, 'b1': cls.bin1, 'b2': cls.bin2,
                   'uuid': cls.xuuid, 'oid': cls.oid}
        cls.lst = [cls.regex, cls.bin1, cls.bin2, cls.xuuid, cls.oid]

    def test_types(self):
        trans = DefaultDocumentFormatter().transform_value

        # regex
        _, patt, flags = trans(self.regex).rsplit("/")
        self.assertIn('x', flags)
        self.assertIn('m', flags)
        self.assertNotIn('l', flags)
        self.assertEqual(patt, 'hello')

        # binary
        self.assertEqual(trans(self.bin1), 'AGhlbGxvAA==')
        self.assertEqual(trans(self.bin2), 'AGhlbGxvAA==')

        # UUID
        self.assertEqual(trans(self.xuuid), self.xuuid.hex)

        # Other type
        self.assertEqual(trans(self.oid), str(self.oid))

        # Compound types
        transformed = trans(self.doc)
        for k, v in self.doc.items():
            self.assertEqual(trans(v), transformed[k])
        for el1, el2 in zip(self.lst, map(trans, self.lst)):
            self.assertEqual(trans(el1), el2)

    def test_default_formatter(self):
        formatter = DefaultDocumentFormatter()

        def check_format(document):
            transformed = dict((k, formatter.transform_value(v))
                               for k, v in document.items())
            self.assertEqual(transformed, formatter.format_document(document))

        # Flat
        check_format(self.doc)

        # Nested
        doc2 = {"nested": self.doc}
        check_format(doc2)

        # With nested lists
        doc3 = {"nested": [[{"inner_doc": [[self.doc]]}]]}
        check_format(doc3)

    def test_flattener(self):
        formatter = DocumentFlattener()

        # Flat already
        transformed = dict((k, formatter.transform_value(v))
                           for k, v in self.doc.items())
        self.assertEqual(transformed, formatter.format_document(self.doc))

        # Nested documents
        nested = {"doc": self.doc}
        transformed2 = formatter.format_document(nested)
        constructed = dict(("doc.%s" % k, formatter.transform_value(v))
                           for k, v in self.doc.items())
        self.assertEqual(transformed2, constructed)

        # With nested lists
        lists = {"doc": [self.doc, {"sub-doc": self.doc}]}
        constructed_a = dict(("doc.0.%s" % k, formatter.transform_value(v))
                             for k, v in self.doc.items())
        constructed_b = dict(("doc.1.sub-doc.%s" % k,
                              formatter.transform_value(v))
                             for k, v in self.doc.items())
        constructed_a.update(constructed_b)
        self.assertEqual(lists, constructed)
