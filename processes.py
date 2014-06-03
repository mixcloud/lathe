"""
Functions for obtaining information about running processes.

These use procfs so will only function on linux or BSD.
"""
import collections
import os
import re

NUMBER = re.compile('^[0-9]+$')


def process_ids():
    return [
        int(pid) for pid in os.listdir('/proc')
        if NUMBER.match(pid)
    ]


def running_processes_by_name(names):
    """
    Get the pids of all running processes with the supplied names.

    Returns a dict mapping process name to list of pids
    """
    names = set(names)
    name_pid_mapping = collections.defaultdict(list)

    for pid in process_ids():
        comm_file = os.path.join('/proc', str(pid), 'comm')
        with open(comm_file, 'r') as f:
            process_name = f.read().strip()
        if process_name in names:
            name_pid_mapping[process_name].append(int(pid))
    return name_pid_mapping


def check_for_open_files(filenames):
    """
    Determines which files there are open handles to on the system.

    filenames is an iterable for full file paths. The function returns
    a pair of sets closed_handles, open_handles where closed_handles is
    the names of all the files which do not have handles open to them, and
    open_handles is the filenames where there is one or more handle open to them
    """
    closed_handles = set(filenames)
    open_handles = set()

    for pid in process_ids():
        fd_directory = os.path.join('/proc', str(pid), 'fd')
        try:
            for fd in os.listdir(fd_directory):
                fd = os.path.join(fd_directory, fd)
                try:
                    fd_target = os.readlink(fd)
                except OSError as exc:
                    # Files may be closed while we are trying to read them
                    if exc.errno != 2:
                        raise
                else:
                    if fd_target in closed_handles:
                        open_handles.add(fd_target)
                        closed_handles.remove(fd_target)
        except OSError as exc:
            # Processes may end while we are examining their files
            if exc.errno != 2:
                raise
    return closed_handles, open_handles
