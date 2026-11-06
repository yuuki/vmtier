"""Bounded evidence for a blob-only outage while the Cold service stays alive."""

import contextlib
from pathlib import Path
import urllib.error


@contextlib.contextmanager
def unavailable_blobs(store, work, s3_proxy=None):
    if store['type'] == 's3':
        # Keep Moto's in-memory objects for restoration. All blob access goes
        # through this proxy, which rejects requests while the outage is active.
        s3_proxy.unavailable.set()
        try:
            yield 'VMTierInjectedBlobOutage'
        finally:
            s3_proxy.unavailable.clear()
        return
    root = Path(store['path'])
    offline = Path(work) / 'objects-offline'
    offline.mkdir()
    moved = []
    try:
        # Local holds an os.Root descriptor: renaming the root itself would
        # leave the running process able to read it. Move its contents instead.
        for path in sorted(root.iterdir()):
            if path.name == '.writer.lock':
                continue
            path.rename(offline / path.name)
            moved.append(path.name)
        assert moved, 'No blob contents were made unavailable'
        yield 'object not found'
    finally:
        for name in moved:
            (offline / name).rename(root / name)


def assert_running_blob_failure(query, process, read_metrics, error_marker):
    assert process.poll() is None, 'Cold process exited before blob outage query'
    before = read_metrics()
    try:
        query()
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        # Stock vmselect may wrap a storage failure as query evaluation HTTP
        # 422. The injected error marker and backend counters establish cause.
        assert error.code == 422 or 500 <= error.code < 600, f'Unexpected blob outage HTTP status: {error.code}'
        assert error_marker in body, f'Query did not report the injected blob error: {body}'
        evidence = {'http_status': error.code, 'error_body': body}
    else:
        raise AssertionError('Blob outage returned a successful query')
    assert process.poll() is None, 'Cold process exited during blob outage query'
    after = read_metrics()
    for metric in ('vmtier_query_requests_total', 'vmtier_query_errors_total',
                   'vmtier_object_get_requests_total'):
        delta = after.get(metric, 0) - before.get(metric, 0)
        assert delta > 0, f'Blob outage query did not increment {metric}'
        evidence[metric + '_delta'] = delta
    return evidence
