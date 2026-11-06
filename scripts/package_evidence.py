#!/usr/bin/env python3
"""Archive CI work directories without changing their original paths or bytes."""

import argparse
from pathlib import Path
import tarfile
import tempfile


def package_evidence(cache_dir):
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    directories = sorted(path for path in cache_dir.glob('ci-*') if path.is_dir())
    destination = cache_dir / 'e2e-evidence.tar.gz'
    # Always produce an archive, even if setup failed before any suite started.
    # Build beside the destination so a failed write cannot leave a partial file
    # (or an earlier successful archive) available for upload.
    destination.unlink(missing_ok=True)
    with tempfile.NamedTemporaryFile(dir=cache_dir, suffix='.tar.gz', delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        with tarfile.open(temporary_path, 'w:gz') as archive:
            for directory in directories:
                archive.add(directory, arcname=directory.name)
        temporary_path.replace(destination)
    finally:
        temporary_path.unlink(missing_ok=True)
    print(f'Packaged {len(directories)} CI work directories into {destination}')
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache-dir', type=Path, default=Path('.cache'))
    args = parser.parse_args()
    package_evidence(args.cache_dir)


if __name__ == '__main__':
    main()
