#!/usr/bin/env python
# coding=utf8

from cStringIO import StringIO
import multiprocessing
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import AWSConnectionError
import logbook
from setproctitle import setproctitle

log = logbook.Logger('backend')


class FilesystemBackend(object):
    def __init__(self, basepath):
        self.basepath = os.path.join(os.path.abspath(basepath))

    def open_backup_archive(self, backup_id, ending='.tar.xz.mob'):
        """Returns a file descriptor to write to for storing the backup
        archive."""

        fn = os.path.join(self.basepath, '%s%s' % (backup_id, ending))
        log.debug('Opened filesystem archive: %s' % fn)
        return os.open(fn, os.O_CREAT | os.O_WRONLY | os.O_EXCL)

    def open_backup_meta(self, backup_id, ending='.mdx.xz.mob'):
        fn = os.path.join(self.basepath, '%s%s' % (backup_id, ending))
        log.debug('Opened filesystem meta: %s' % fn)
        return os.open(fn, os.O_CREAT | os.O_WRONLY | os.O_EXCL)


class BotoBackend(object):
    # see: http://docs.amazonwebservices.com/AmazonS3/latest/dev/qfacts.html
    MAX_FILESIZE = 5 * 1024 ** 4  # 5 TB when using multi-upload
    MULTI_UPLOAD_THRESHOLD = 5 * 1024 ** 2  # 5 MB minimum size
    MULTI_UPLOAD_MAX_PARTS = 10000  # parts are numbered 1-10000 (inclusive!)
    MULTI_UPLOAD_MIN_FILE_SIZE = 5 ** 1024 * 2

    def __init__(self, access_key,
                       secret_key,
                       bucket_name,
                       prefix,
                       pool_size=10):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.pool_size = pool_size
        self.num_retries = 10

    def _calc_part_size(self, total_size):
        if total_size > self.MAX_FILESIZE:
            raise ValueError(
                'Total expected size of %d exceeds maximum size of %d'\
                % (total_size, self.MAX_FILESIZE)
            )

        if total_size < MULTI_UPLOAD_THRESHOLD:
            return None  # none == use single file upload

        part_size = total_size // self.MULTI_UPLOAD_MAX_PARTS
        if total_size % self.MULTI_UPLOAD_MAX_PARTS:
            # does not divide evenly
            part_size += 1

        if part_size < self.MULTI_UPLOAD_MIN_FILE_SIZE:
            part_size = self.MULTI_UPLOAD_MIN_FILE_SIZE

        log.debug('Calculated part size %d from total size %d' % (
            part_size, total_size
        ))

        return part_size

    def _initialize_workers(self, queue_size, *args):
        log.debug('Initializing new set of workers')
        self._part_queue = multiprocessing.JoinableQueue(queue_size)
        self.workers = []

        for i in xrange(self.pool_size):
            w = multiprocessing.Process(target=self._worker, args=args)
            w.daemon = True
            w.start()
            self.workers.append(w)

    def _mp_from_id(id, key_name):
        bucket = self._open_boto_bucket()
        mp = boto.s3.multipart.MultiPartUpload(bucket)
        mp.id = id
        mp.key_name = key_name

        return mp

    def _join_workers(self):
        n = len(self.workers)
        for i in n:
            self._part_queue.put(None)

        log.debug('Joining %d workers' % n)
        self._part_queue.join()
        self.workers = []

    def _open_boto_bucket(self):
        conn = S3Connection(self.access_key, self.secret_key)
        bucket = conn.get_bucket(self.bucketname)
        log.debug('Opened S3 connection, bucket "%s"' % self.bucketname)

        return bucket

    def _upload_fd(self, key_name, fd, expected_size):
        bucket = self._open_boto_bucket()
        part_size = self._calc_part_size(expected_size)

        # open fd for reading, this is means it will probably be closed
        # once this function returns
        inp = os.fdopen(fd, 'rb')

        # fill buffer
        if not part_size:
            buf = inp.read()
        else:
            buf = inp.read(part_size)

        if part_size and len(buf) >= self.MULTI_UPLOAD_MIN_FILE_SIZE:
            log.debug('Uploading fd %d using multipart uploading '\
                      '(part_size %d, expected_size %d)' % (
                           fd, part_size, expected_size
                      ))

            part_num = 1
            mp = bucket.initialize_multipart_upload(key_name)

            self._initialize_workers(
                # try not to use more than 200 MB of ram if possible
                10 if part_size < 10 * 1024 ** 2 else 1,
                mp.id,
                mp.key_name
            )

            while buf:
                # queue upload
                self._part_queue.put((part_num, buf))
                part_num += 1

                buf = inp.read(self.part_size)

            self._join_workers()
            mp.complete_upload()
        else:
            log.debug('Uploading fd %d to "%s" using normal upload' %
                fd, key_name
            )

            k = Key(self.bucket)
            k.key = key_name
            k.set_contents_from_string(buf)

        log.debug('Done uploading')

    def _worker(self, multipart_id, key_name):
        setproctitle('mob s3 upload worker')

        bucket = self._open_boto_bucket()

        # find multipart upload
        mp = self._mp_from_id(multipart_id, key_name)

        while True:
            part_num, part_data = self._part_queue.get()

            n = self.num_retries
            while i<self.num_retries:
                log.debug('Transfering part %d (attempt %d)' % (
                    part_num, self.num_retries-n+1)
                )

                try:
                    mp.upload_part_from_file(StringIO(part_data), part_num)
                    break
                except AWSConnectionError, e:
                    log.warning('Transfer of part %d of "%s" '\
                                'failed (%d retries left): %s'\
                    % (part_num, key_name, n, str(e)))
                    n -= 1

        log.debug('Done transfering part %d' % part_num)
