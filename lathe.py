#!/usr/bin/env python
"""
lathe - python log rotation script
"""

usage_information = '''lathe - python logrotate alternative with s3 integration

usage: lathe config.json
'''

import contextlib
import datetime
import errno
import fcntl
import fnmatch
import gzip
import json
import logging
import os
import shutil
import signal
import sys

from s3logstore import S3LogStore
from processes import running_processes_by_name, check_for_open_files
from throttle import Throttle

logger = logging.getLogger('mixcloud.lathe')


def compress_file(filepath):
    """
    Compresses the supplied file, deleting the original
    """
    gzip_path = filepath + '.gz'
    with gzip.open(gzip_path, 'w') as compressed_file:
        with open(filepath) as raw_file:
            shutil.copyfileobj(raw_file, compressed_file)
    os.unlink(filepath)
    return gzip_path


@contextlib.contextmanager
def request_lock(lockfile):
    with open(lockfile, 'a') as lock_handle:
        try:
            fcntl.lockf(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                yield False
            raise
        else:
            yield True


def rotate_log_files(options):
    with request_lock(options['lock_file']) as acquired:
        if not acquired:
            logger.warn('Not rotating, previous job still underway')
            return

        files_to_rotate = [
            file for file in os.listdir(options['log_directory'])
            if fnmatch.fnmatch(file, options['filename_filter'])
        ]

        rotation_suffix = datetime.datetime.now().strftime(options['timestamp_format'])

        filename_mapping = {
            file: file + rotation_suffix
            for file in files_to_rotate
        }

        # Move all files
        rotated_files = []
        for original_name, rotated_name in filename_mapping.items():
            original_path = os.path.join(options['log_directory'], original_name)
            rotated_path = os.path.join(options['log_directory'], rotated_name)
            if not os.path.exists(rotated_path):
                os.rename(original_path, rotated_path)
                rotated_files.append(rotated_name)
            else:
                logger.warning('Did not rotate file. File called %s already existed', rotated_path)

        # Run kick commands
        pids_for_processes = running_processes_by_name(options['reopen_file_signals'].keys())
        for process_name, signal_name in options['reopen_file_signals'].items():
            signal_id = getattr(signal, 'SIG' + signal_name.upper())
            pids = pids_for_processes[process_name]
            for pid in pids:
                os.kill(pid, signal_id)

        throttle_file_checks = Throttle(5)
        s3_store = S3LogStore(options)

        # Get files which have no open handles and process them as soon as we can.
        # Files with open handles wait until next time through the loop. We throttle
        # to avoid checking too often.
        # TODO: Should we also pick up and retry copying any gz files which we could not
        #       copy to s3 last time around?
        open_files = rotated_files
        while open_files:
            throttle_file_checks.wait()
            closed_files, open_files = check_for_open_files(open_files)
            for ready_file in closed_files:
                try:
                    ready_path = os.path.join(options['log_directory'], ready_file)

                    shutil.copy(
                        ready_path,
                        os.path.join(options['spool_directory'], ready_file))

                    compressed_path = compress_file(ready_path)
                    s3_store.store_file(compressed_path)
                    os.unlink(compressed_path)
                except:
                    logger.error('Unexpected error processing %s', ready_file, exc_info=True)


def main(args):
    if len(args) != 1:
        print usage_information
        sys.exit(-1)
    config_file = args[0]
    with open(config_file) as f:
        options = json.load(f)
    rotate_log_files(options)


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler(sys.stderr))
    logger.setLevel(logging.WARNING)
    main(sys.argv[1:])