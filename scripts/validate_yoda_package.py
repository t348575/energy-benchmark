#!/usr/bin/env python3
"""Validate a decompressed tar stream against its source SHA-256 manifest."""

import argparse
import hashlib
import re
import sys
import tarfile


CHUNK_SIZE = 4 * 1024 * 1024
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class HashingReader:
    def __init__(self, source):
        self.source = source
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        data = self.source.read(size)
        if data:
            self.digest.update(data)
        return data


def decode_escaped_filename(value):
    result = []
    index = 0
    while index < len(value):
        if value[index] != "\\":
            result.append(value[index])
            index += 1
            continue
        index += 1
        if index >= len(value):
            raise ValueError("raw manifest ends with an incomplete escape")
        escaped = value[index]
        if escaped == "\\":
            result.append("\\")
        elif escaped == "n":
            result.append("\n")
        elif escaped == "r":
            result.append("\r")
        else:
            raise ValueError("raw manifest contains an unknown filename escape")
        index += 1
    return "".join(result)


def normalize_path(path):
    while path.startswith("./"):
        path = path[2:]
    if path.endswith("/"):
        path = path.rstrip("/")
    return path


def load_manifest(path):
    expected = {}
    with open(path, "r", encoding="utf-8", newline="") as manifest:
        for line_number, raw_line in enumerate(manifest, 1):
            line = raw_line.rstrip("\n")
            if line.endswith("\r"):
                line = line[:-1]
            escaped = line.startswith("\\")
            if escaped:
                line = line[1:]
            if len(line) < 66 or line[64:66] not in ("  ", " *"):
                raise ValueError(
                    "invalid raw manifest record at line {0}".format(line_number)
                )
            digest = line[:64]
            if not SHA256_PATTERN.fullmatch(digest):
                raise ValueError(
                    "invalid SHA-256 at raw manifest line {0}".format(line_number)
                )
            filename = line[66:]
            if escaped:
                filename = decode_escaped_filename(filename)
            filename = normalize_path(filename)
            if not filename or filename in expected:
                raise ValueError(
                    "empty or duplicate path at raw manifest line {0}".format(
                        line_number
                    )
                )
            expected[filename] = digest
    return expected


def validate_member_path(name, allowed_roots):
    if name.startswith("/"):
        raise ValueError("archive contains an absolute path: {0}".format(name))
    normalized = normalize_path(name)
    if not normalized or normalized == ".":
        return normalized
    components = normalized.split("/")
    if any(component in ("", ".", "..") for component in components):
        raise ValueError("archive contains an unsafe path: {0}".format(name))
    if components[0] not in allowed_roots:
        raise ValueError("archive contains an unexpected root: {0}".format(name))
    return normalized


def validate_stream(source, manifest_path, expected_tar_sha256, allowed_roots):
    expected = load_manifest(manifest_path)
    expected_count = len(expected)
    seen = set()
    reader = HashingReader(source)

    with tarfile.open(fileobj=reader, mode="r|") as archive:
        for member in archive:
            path = validate_member_path(member.name, allowed_roots)
            if not path or member.isdir():
                continue
            if not member.isreg():
                raise ValueError(
                    "archive contains a link or special member: {0}".format(member.name)
                )
            if path in seen:
                raise ValueError("archive contains a duplicate file: {0}".format(path))
            if path not in expected:
                raise ValueError("archive contains an unexpected file: {0}".format(path))

            extracted = archive.extractfile(member)
            if extracted is None:
                raise ValueError("could not read archive member: {0}".format(path))
            file_digest = hashlib.sha256()
            total = 0
            while True:
                chunk = extracted.read(CHUNK_SIZE)
                if not chunk:
                    break
                file_digest.update(chunk)
                total += len(chunk)
            if total != member.size:
                raise ValueError("archive member is truncated: {0}".format(path))
            if file_digest.hexdigest() != expected[path]:
                raise ValueError("file SHA-256 mismatch: {0}".format(path))
            seen.add(path)

    # tarfile stops at the end-of-archive marker. Drain the pipe so zstd must
    # decode every concatenated frame and the tar-stream digest covers padding.
    while reader.read(CHUNK_SIZE):
        pass

    missing = sorted(set(expected) - seen)
    if missing:
        raise ValueError(
            "archive is missing {0} file(s), first: {1}".format(len(missing), missing[0])
        )
    actual_tar_sha256 = reader.digest.hexdigest()
    if actual_tar_sha256 != expected_tar_sha256:
        raise ValueError(
            "decompressed tar SHA-256 mismatch: expected {0}, got {1}".format(
                expected_tar_sha256, actual_tar_sha256
            )
        )
    return actual_tar_sha256, expected_count, len(seen)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--expected-tar-sha256", required=True)
    parser.add_argument("--allowed-root", action="append", default=[])
    args = parser.parse_args()

    if not SHA256_PATTERN.fullmatch(args.expected_tar_sha256):
        parser.error("--expected-tar-sha256 must be 64 lowercase hexadecimal digits")
    if not args.allowed_root:
        parser.error("at least one --allowed-root is required")

    try:
        actual, expected_files, verified_files = validate_stream(
            sys.stdin.buffer,
            args.manifest,
            args.expected_tar_sha256,
            set(args.allowed_root),
        )
        print(
            "{0}\t{1}\t{2}".format(actual, expected_files, verified_files)
        )
        return 0
    except (OSError, tarfile.TarError, ValueError) as error:
        print("ERROR: validate_yoda_package: {0}".format(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
