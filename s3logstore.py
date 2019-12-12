import boto
import boto.s3
import contextlib
import os
import socket


class S3LogStore(object):
    """
    Uploads files to an s3 bucket with appropriate names and locations.
    """
    def __init__(self, options):
        self.options = options

    def store_file(self, filepath):
        """
        Store the file at the supplied path as a log in s3.

        Raises an exception if the file is not copied successfully
        """
        with self._bucket() as bucket:
            target_path = self.options['s3_pattern'] % {
                'hostname': socket.gethostname().split('.')[0],
                'log_name': os.path.basename(filepath),
            }
            key = bucket.new_key(target_path)
            key.set_contents_from_filename(filepath)

    @contextlib.contextmanager
    def _bucket(self):
        connection = boto.connect_s3(
            self.options['aws_access_key'],
            self.options['aws_secret_key'])
        try:
            bucket = connection.get_bucket(self.options['s3_bucket'], validate=False)
            yield bucket
        finally:
            connection.close()
