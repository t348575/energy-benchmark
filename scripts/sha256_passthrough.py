#!/usr/bin/env python3
"""Copy stdin to stdout while atomically recording its SHA-256 digest."""

import argparse
import hashlib
import os
import signal
import sys


CHUNK_SIZE = 4 * 1024 * 1024


def write_all(destination, data):
    view = memoryview(data)
    offset = 0
    while offset < len(view):
        written = destination.write(view[offset:])
        if written is None:
            written = len(view) - offset
        if written <= 0:
            raise OSError("stdout made no progress")
        offset += written


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest-file", required=True)
    args = parser.parse_args()

    if hasattr(signal, "SIGPIPE"):
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    digest = hashlib.sha256()
    temporary = "{0}.tmp.{1}".format(args.digest_file, os.getpid())
    try:
        while True:
            chunk = sys.stdin.buffer.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
            write_all(sys.stdout.buffer, chunk)
        sys.stdout.buffer.flush()
        with open(temporary, "x", encoding="ascii") as output:
            output.write(digest.hexdigest() + "\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, args.digest_file)
        return 0
    except Exception as error:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        print("ERROR: sha256_passthrough: {0}".format(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
