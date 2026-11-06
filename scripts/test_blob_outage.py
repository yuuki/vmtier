"""Prevent connection errors and unrelated timeouts from proving a blob outage."""

import io
import os
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import Mock

from blob_outage import assert_running_blob_failure, unavailable_blobs


class BlobOutageTest(unittest.TestCase):
    def check_failure(self, failure, after=None):
        before = {'vmtier_query_requests_total': 10, 'vmtier_query_errors_total': 1,
                  'vmtier_object_get_requests_total': 20}
        if after is None:
            after = {key: value + 1 for key, value in before.items()}
        return assert_running_blob_failure(Mock(side_effect=failure), Mock(poll=lambda: None),
                                           Mock(side_effect=[before, after]), 'object not found')

    def http_error(self, body):
        return urllib.error.HTTPError('http://localhost/query', 503, 'failed', {}, io.BytesIO(body))

    def test_running_backend_read_failure_is_recorded(self):
        evidence = self.check_failure(self.http_error(b'query failed: object not found'))
        self.assertEqual(evidence['http_status'], 503)
        self.assertEqual(evidence['vmtier_object_get_requests_total_delta'], 1)

    def test_connection_and_timeout_failures_are_not_accepted(self):
        for error in (urllib.error.URLError('connection refused'), TimeoutError(),
                      self.http_error(b'connection refused'), self.http_error(b'query timed out')):
            with self.subTest(error=error), self.assertRaises((AssertionError, urllib.error.URLError, TimeoutError)):
                self.check_failure(error)

    def test_missing_blob_attempt_evidence_is_not_accepted(self):
        after = {'vmtier_query_requests_total': 11, 'vmtier_query_errors_total': 2,
                 'vmtier_object_get_requests_total': 20}
        with self.assertRaisesRegex(AssertionError, 'vmtier_object_get_requests_total'):
            self.check_failure(self.http_error(b'object not found'), after)

    def test_local_outage_hides_objects_from_an_open_directory_descriptor_and_restores(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / 'objects'
            path = root / 'tenants/1:2/index.json'
            path.parent.mkdir(parents=True)
            path.write_bytes(b'original immutable object')
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaisesRegex(RuntimeError, 'query failed'):
                    with unavailable_blobs({'type': 'local', 'path': str(root)}, Path(temporary)):
                        with self.assertRaises(FileNotFoundError):
                            os.open('tenants/1:2/index.json', os.O_RDONLY, dir_fd=directory)
                        raise RuntimeError('query failed')
                self.assertEqual(path.read_bytes(), b'original immutable object')
            finally:
                os.close(directory)


if __name__ == '__main__':
    unittest.main()
