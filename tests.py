import contextlib
import os
import subprocess
import tempfile
import unittest

from lathe import compress_file
from processes import check_for_open_files, process_ids, running_processes_by_name
from throttle import Throttle


@contextlib.contextmanager
def running_process(path):
    try:
        process = subprocess.Popen(
            path,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        yield process.pid
    finally:
        process.kill()


class RunningProcessesTestCase(unittest.TestCase):
    def test_should_identify_running_process(self):
        with running_process('/bin/cat') as pid:
            running_processes = running_processes_by_name(['cat'])
            self.assertIn(pid, running_processes['cat'])

    def test_should_handle_multiple_processes_same_name(self):
        with running_process('/bin/cat') as pid:
            with running_process('/bin/cat') as pid2:
                running_processes = running_processes_by_name(['cat'])
                self.assertIn(pid, running_processes['cat'])
                self.assertIn(pid2, running_processes['cat'])

    def test_should_handle_processes_with_arguments(self):
        with running_process(['/bin/cat', 'foo', '-']) as pid:
            running_processes = running_processes_by_name(['cat'])
            self.assertIn(pid, running_processes['cat'])

    def test_should_return_empty_list_when_no_process_running(self):
        with running_process('/bin/cat'):
            running_processes = running_processes_by_name(['there_is_no_process_with_this_name'])
            self.assertNotIn('there_is_no_process_with_this_name', running_processes)


class CheckForOpenFilesTestCase(unittest.TestCase):
    def test_should_report_file_open(self):
        with tempfile.NamedTemporaryFile() as f:
            closed_files, open_files = check_for_open_files([f.name])
            self.assertIn(f.name, open_files)
            self.assertEqual(len(closed_files), 0)

    def test_should_report_file_not_open(self):
        with tempfile.NamedTemporaryFile() as f:
            filename = f.name
        closed_files, open_files = check_for_open_files([filename])
        self.assertIn(filename, closed_files)
        self.assertEqual(len(open_files), 0)

    def test_should_change_when_file_closed(self):
        with tempfile.NamedTemporaryFile() as f:
            filename = f.name
            closed_files, open_files = check_for_open_files([filename])
            self.assertIn(filename, open_files)
            self.assertEqual(len(closed_files), 0)
        closed_files, open_files = check_for_open_files([filename])
        self.assertIn(filename, closed_files)
        self.assertEqual(len(open_files), 0)


class ProcessIdsTestCase(unittest.TestCase):
    def test_gets_process_pid(self):
        with running_process('/bin/cat') as pid:
            self.assertIn(pid, process_ids())


class CompressFileTestCase(unittest.TestCase):
    def test_returns_compressed_name(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write('This is a file which will be compressed')
        compressed_name = compress_file(f.name)
        self.assertEqual(compressed_name, f.name + '.gz')
        os.unlink(compressed_name)

    def test_creates_compressed_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write('This is a file which will be compressed')
        compressed_name = compress_file(f.name)
        self.assertTrue(os.path.isfile(compressed_name))
        os.unlink(compressed_name)

    def test_deletes_original_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write('This is a file which will be compressed')
        compressed_name = compress_file(f.name)
        self.assertFalse(os.path.exists(f.name))
        os.unlink(compressed_name)


class MockTime(object):
    def __init__(self):
        self._time = 1
        self.sleep_duration = None

    def clock(self):
        return self._time

    def sleep(self, duration):
        self.sleep_duration = duration


class ThrottleTestCase(unittest.TestCase):
    def setUp(self):
        self.mock_time = MockTime()
        self.test_throttle = Throttle(10, time=self.mock_time)

    def test_first_time_should_run_immediately(self):
        self.test_throttle.wait()
        self.assertEqual(self.mock_time.sleep_duration, None)

    def test_should_sleep_minimum_interval(self):
        self.test_throttle.wait()
        self.test_throttle.wait()
        self.assertEqual(self.mock_time.sleep_duration, 10)

    def test_should_sleep_partial_time(self):
        self.mock_time._time = 5
        self.test_throttle.wait()
        self.mock_time._time = 10
        self.test_throttle.wait()
        self.assertEqual(self.mock_time.sleep_duration, 5)
