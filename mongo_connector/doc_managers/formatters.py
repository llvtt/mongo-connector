import base64

import bson

from mongo_connector.compat import PY3

if PY3:
    long = int


class DocumentFormatter(object):
    """Interface for classes that can transform documents to conform to
    external drivers' expectations.
    """

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
    """Basic DocumentFormatter that preserves numbers, base64-encodes binary,
    and stringifies everything else.
    """

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
            for key in document:
                value = document[key]
                yield self.transform_element(key, value)
        return dict(_kernel(document))


class DocumentFlattener(DefaultDocumentFormatter):
    """Formatter that completely flattens documents and unwinds arrays:

    An example:
      {"a": 2,
       "b": {
         "c": {
           "d": 5
         }
       },
       "e": [6, 7, 8]
      }

    becomes:
      {"a": 2, "b.c.d": 5, "e.0": 6, "e.1": 7, "e.2": 8}

    """

    def transform_value(self, value):
        if isinstance(value, dict):
            return self.transform_document(value)
        elif isinstance(value, list):
            return [self.transform_value(v) for v in value]
        elif isinstance(value, (float, long, int)):
            return value
        elif isinstance(value, bson.Binary):
            return base64.b64encode(value)
        return str(value)

    def transform_element(self, key, value):
        if isinstance(value, list):
            for li, lv in enumerate(value):
                for inner_k, inner_v in self.transform_element(
                        "%s.%s" % (key, li), lv):
                    yield inner_k, inner_v
        elif isinstance(value, dict):
            formatted = self.format_document(value)
            for doc_key in formatted:
                yield "%s.%s" % (key, doc_key), formatted[doc_key]
        else:
            yield key, self.transform_value(value)

    def format_document(self, document):
        def flatten(doc, path):
            top_level = (len(path) == 0)
            if not top_level:
                path_string = ".".join(path)
            for k in doc:
                v = doc[k]
                if isinstance(v, dict):
                    path.append(k)
                    for inner_k, inner_v in flatten(v, path):
                        yield inner_k, inner_v
                    path.pop()
                else:
                    transformed = self.transform_element(k, v)
                    for new_k, new_v in transformed:
                        if top_level:
                            yield new_k, new_v
                        else:
                            yield "%s.%s" % (path_string, new_k), new_v
        return dict(flatten(document, []))
