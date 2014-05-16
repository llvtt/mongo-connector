import base64

import bson

from mongo_connector.compat import PY3

if PY3:
    long = int


class DocumentFormatter(object):

    def transform_value(self, value):
        """Transform a leaf-node in a document.

        This method may be overridden to provide custom handling for specific
        types of values.
        """
        raise NotImplementedError

    def transform_element(self, key, value):
        """Transform a single key-value pair within a document.

        This method may be overridden to provide custom handling for specific
        types of values. This method should return an iterator over the
        resulting key-value pairs.
        """
        raise NotImplementedError

    def format_document(self, document):
        """Format a document in preparation to be sent to an external driver."""
        raise NotImplementedError


class DefaultDocumentFormatter(DocumentFormatter):

    def __init__(self):
        # Map type -> transformation function for that type
        identity = lambda x: x
        self.types_mapping = {
            bson.Binary: base64.b64encode,
            list: lambda l: [self.types_mapping[type(el)] for el in l],
            dict: self.format_document,
            int: identity,
            long: identity,
            float: identity
        }

    def transform_value(self, value):
        transformer = self.types_mapping.get(type(value))
        if transformer is not None:
            return transformer(value)
        # Default
        return str(value)

    def transform_element(self, key, value):
        yield key, self.transform_value(value)

    def format_document(self, document):
        def _kernel(doc):
            for key, value in document.items():
                yield self.transform_element(key, value)
        return dict(_kernel(document))


class DocumentFlattener(DefaultDocumentFormatter):

    def transform_element(self, key, value):
        if isinstance(value, list):
            for li, lv in enumerate(value):
                for inner_k, inner_v in self.transform_element(
                        "%s.%d" % (key, li), lv):
                    yield inner_k, inner_v
        elif isinstance(value, dict):
            transformd = self.format_document(value)
            for doc_key in transformd:
                yield "%s.%s" % (key, doc_key), transformd[doc_key]
        else:
            yield key, self.transform_value(value)

    def format_document(self, document):
        def flatten(doc, path):
            for k, v in doc:
                v = doc[k]
                path.append(k)
                if isinstance(v, dict):
                    for inner_k, inner_v in flatten(v, path):
                        yield inner_k, inner_v
                else:
                    transformd = self.transform_element(k, v)
                    for new_k, new_v in transformd:
                        if len(path) > 1:
                            yield ".".join(path), new_v
                        else:
                            yield new_k, new_v
                path.pop()
        return dict(flatten(document, []))
