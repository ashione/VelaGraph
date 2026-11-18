import io
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import check_source_archive as check


class SourceArchiveTests(unittest.TestCase):
    def archive(self, changes=(), omit=None):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = pathlib.Path(directory.name) / 'source.tar'
        with tarfile.open(path, 'w') as archive:
            for name in check.REQUIRED:
                if name != omit:
                    entry = tarfile.TarInfo(name)
                    entry.size = 1
                    archive.addfile(entry, io.BytesIO(b'x'))
            for entry, body in changes:
                archive.addfile(entry, io.BytesIO(body) if body is not None else None)
        return path

    def entry(self, name, body=b'x', kind=tarfile.REGTYPE):
        entry = tarfile.TarInfo(name)
        entry.type = kind
        entry.size = len(body)
        return entry, body

    def test_complete_finite_profile_and_checksum(self):
        result = check.inspect_archive(self.archive())
        self.assertEqual(result['files'], len(check.REQUIRED))
        self.assertEqual(len(result['archive_sha256']), 64)
        self.assertFalse(result['build_executed'])
        self.assertFalse(result['release_accepted'])

    def test_each_required_file_missing(self):
        for name in check.REQUIRED:
            with self.subTest(name=name), self.assertRaises(ValueError):
                check.inspect_archive(self.archive(omit=name))

    def test_empty_required_file(self):
        with self.assertRaises(ValueError):
            check.inspect_archive(self.archive([self.entry('LICENSE', b'')], omit='LICENSE'))

    def test_paths_links_special_files_duplicates(self):
        bad = [self.entry(name) for name in (
            '/absolute', '../escape', 'a/../escape', './alias', 'a//alias',
            '.git/config', 'a/.git/config', 'a\\b', 'C:/absolute', 'LICENSE')]
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE,
                     tarfile.CHRTYPE, tarfile.BLKTYPE):
            bad.append(self.entry('special', b'', kind))
        for entry in bad:
            with self.subTest(name=entry[0].name, kind=entry[0].type), self.assertRaises(ValueError):
                check.inspect_archive(self.archive([entry]))

    def test_regular_file_trailing_slash_is_not_required_file(self):
        for name in ('LICENSE/', 'LICENSE//'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                check.inspect_archive(self.archive([self.entry(name)], omit='LICENSE'))

    def test_file_directory_collisions(self):
        for entries in ([self.entry('plain'), self.entry('plain/child')],
                        [self.entry('plain/child'), self.entry('plain')],
                        [self.entry('plain'), self.entry('plain/child', b'', tarfile.DIRTYPE)]):
            with self.assertRaises(ValueError):
                check.inspect_archive(self.archive(entries))

    def test_cli_failure_has_no_success_report(self):
        result = subprocess.run([sys.executable, check.__file__, str(self.archive(omit='LICENSE'))],
                                capture_output=True, text=True, timeout=5)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, '')
        self.assertIn('source archive rejected', result.stderr)

    def test_limits(self):
        path = self.archive()
        for setting in ('MAX_ARCHIVE_BYTES', 'MAX_FILE_BYTES',
                        'MAX_TOTAL_BYTES', 'MAX_ENTRIES'):
            with self.subTest(setting=setting), patch.object(check, setting, 0), self.assertRaises(ValueError):
                check.inspect_archive(path)

    def test_complete_archive_termination(self):
        original = self.archive().read_bytes()
        extra = self.archive([self.entry('../escape')]).read_bytes()
        with tarfile.open(fileobj=io.BytesIO(original), mode='r:') as archive:
            for _ in archive:
                pass
            end = archive.offset
        for suffix in (b'', bytes(512), bytes(1023), bytes(1025), b'bad data', extra):
            with self.subTest(suffix_bytes=len(suffix)), self.assertRaises(ValueError):
                path = self.archive()
                path.write_bytes(original[:end] + suffix)
                check.inspect_archive(path)
        for suffix in (b'bad data', extra):
            with self.subTest(appended_bytes=len(suffix)), self.assertRaises(ValueError):
                path = self.archive()
                path.write_bytes(original + suffix)
                check.inspect_archive(path)
        path = self.archive()
        path.write_bytes(original[:end] + bytes(1024))
        self.assertEqual(check.inspect_archive(path)['files'], len(check.REQUIRED))

    def test_malformed_and_truncated_payload(self):
        path = self.archive()
        path.write_bytes(b'not a tar')
        with self.assertRaises(tarfile.TarError):
            check.inspect_archive(path)
        path = self.archive()
        path.write_bytes(path.read_bytes()[:513])
        with self.assertRaises((ValueError, tarfile.TarError)):
            check.inspect_archive(path)


if __name__ == '__main__':
    unittest.main()
