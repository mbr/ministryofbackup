#!/usr/bin/env python
# coding=utf8

from collections import namedtuple
from hashlib import sha1
import mmap
import os
import stat
import sys
import time
from urllib import unquote
from urlparse import urlparse

import msgpack
import logbook
import progressbar
from remember.memoize import memoize, memoized_property

from backend import FilesystemBackend, BotoBackend

log = logbook.Logger(__name__)

DATA_PROGRESS_BAR = ['Complete: ', progressbar.Percentage(), ' ',
                        progressbar.Bar(marker='#', left='[', right=']'),
                        ' ', progressbar.ETA(), ' ',
                        progressbar.FileTransferSpeed()]


def backend_url(v):
    o = urlparse(v)

    # FIXME: support gs by adding scheme and a way to disable multipart
    #        uploading
    if not o.scheme in ('file', 's3'):
        raise ValueError('Unknown scheme: %s' % o.scheme)

    if 's3' == o.scheme:
        if not o.netloc:
            raise ValueError('No bucket name specified: %s' % v)

    log.debug('Parsed url %r to %r' % (v, o))
    return o


def create_backend(urldata):
    if 'file' == urldata.scheme:
        return FilesystemBackend(urldata.path)
    elif 's3' == urldata.scheme:
        pw = unquote(urldata.password or '')
        #log.debug('S3 secret key: %s' % pw)
        return BotoBackend(
            access_key=unquote(urldata.username),
            secret_key=unquote(pw),
            bucket_name=unquote(urldata.hostname),
            prefix=unquote(urldata.path)
        )


class HashReadWrap(object):
    def __init__(self, fileobj, hashfunc=sha1):
        self.h = hashfunc()
        self.fileobj = fileobj
        self.eofreached = False

    def read(self, size=-1):
        buf = self.fileobj.read(size)
        if not buf:
            self.eofreached = True
        else:
            self.h.update(buf)

        return buf

    def close(self):
        return self.fileobj.close()

    @property
    def closed(self):
        return self.fileobj.closed


class MetaBase(object):
    """An object representing the metadata of an entity on the filesystem.

    :param path: The absolute path
    """

    def __init__(self, path):
        self.path = path
        self.children = []

    @memoized_property
    def s(self):
        return os.lstat(self.path)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.path)


MetaTuple = namedtuple('MetaTuple', ['mode', 'uid', 'gid', 'size', 'atime',
                                     'mtime', 'ctime'])


class FileMeta(MetaBase):
    read_buf_size = 1024*1024*4  # 4M should be sufficient for speed and not
                                 # too memory hungry

    @memoized_property
    def content_print(self):
        """Content prints rely only on the contents of the file - pretty much a
        'normal' application of the underlying hash function"""
        if hasattr(self, '_fileobj'):
            if self._fileobj.eofreached:
                return self._fileobj.h.digest()
            else:
                log.warning('Hash of file required after partial read: %s' %\
                            self.path)

        ftype = stat.S_IFMT(self.s.st_mode)

        if stat.S_IFLNK == ftype:
            return ''
        elif stat.S_IFREG == ftype:
            h = sha1()
            remain = self.filesize

            with open(self.path, 'rb') as src:
                buf = True
                while remain:
                    buf = src.read(min(self.read_buf_size, remain))
                    remain -= len(buf)
                    h.update(buf)

            return h.digest()
        else:
            raise Exception('Cannot handle filetype %s - sorry' % ftype)

    @memoized_property
    def filesize(self):
        """Returns the filesize in bytes"""
        return self.s.st_size

    @memoized_property
    def meta_print(self):
        """The meta print is a fingerprint based solely on the metadata of the
        file, not the contents"""

        stat_string = ' '.join(map(str, iter(self.s)))
        return sha1(stat_string).digest()

    @memoized_property
    def meta_tuple(self):
        return MetaTuple(
            mode=self.s.st_mode,
            uid=self.s.st_uid,
            gid=self.s.st_gid,
            size=self.s.st_size,
            atime=self.s.st_atime,
            mtime=self.s.st_mtime,
            ctime=self.s.st_ctime,
        )

    def open_read(self):
        self._fileobj = HashReadWrap(open(self.path, 'rb'), sha1)

        return self._fileobj

class DirMeta(MetaBase):
    pass


class Database(object):
    """Database object.

    The database stores checksums of all files metadata and contents.
    Subsequent runs can then compared the existing files to these collected
    checksums and ensure only changes are backed up again.

    A database keeps relative paths only. The folder being upload can therefore
    be moved elsewhere.

    :param base: The base path for the folder to be backed up. **Must** be an
                 absolute path.
    """
    def __init__(self, base):
        self.base = base
        self.meta_prints = {}
        self.content_prints = {}

    def dump(self, outfile):
        """Write a serialized version of the database to filehandle."""
        db_dict = {
            'meta_prints': self.meta_prints,
            'content_prints': self.content_prints
        }
        msgpack.dump(db_dict, outfile)

    def get_altered_files(self, fileset=None, progress=None):
        """Return a list of all files that have been altered (in comparison
        with the loaded checksums).

        An altered file is one that has had its contents changed.

        :param fileset: If suppled, instead of checking all files, only check
        the files whose relative names are in this list.
        :param progress: Progress callback, called after every file is
        processed with the total number of bytes checked so far.
        :param return: List of relative names of files that have been altered.
        """
        fileset = fileset or self.files.keys()

        altered = []

        n_bytes = 0
        for rel_name in fileset:
            f = self.files[rel_name]
            if rel_name in self.content_prints:
                if f.content_print != self.content_prints[rel_name]:
                    altered.append(rel_name)

            if progress:
                n_bytes += f.filesize
                progress(n_bytes)

        return altered

    def get_deleted_files(self):
        """Return a list of all files that are no longer present but have
        records in the database.

        :param return: List of relative names of files that have been deleted.
        """
        return [rel_name for rel_name in self.meta_prints.iterkeys() if rel_name
        not in self.files]

    def get_new_and_updated_files(self, progress=None):
        """Return a list of all files whose metadata (stat) has changed.

        :param progress: Progress callback, called after every file with the
                         number of files processed.
        :param return: List of relative names of files that have new metadata.
        """
        # shortcut for empty db
        if not self.meta_prints:
            return self.files.keys(), []

        new = []
        updated = []

        n_files = 0
        for rel_name, file_meta in self.files.iteritems():
            if progress:
                progress(n_files)
                n_files += 1

            if not rel_name in self.meta_prints:
                new.append(rel_name)
                continue
            if self.meta_prints[rel_name] != file_meta.meta_print:
                updated.append(rel_name)

        return new, updated

    def get_sizes_of(self, fileset=None):
        """Calculate the total size in bytes of all files in a set.

        :param fileset: Relative names to files whose sizes are to be summed.
                        If None, sum over all files.
        :return: Sum of filesizes in bytes.
        """

        if None == fileset:
            return sum(fm.filesize for fm in self.files.itervalues())

        return sum(self.files[rel_name].filesize for rel_name in fileset)

    @classmethod
    def load(cls, base, infile):
        """Unserialize a database from file.

        :param base: Base path that all files are supposedly relative to.
        :param infile: File object to read from.

        :return: A :py:class:Database instance.
        """
        db_dict = msgpack.load(infile)

        db = cls(base)
        db.meta_prints = db_dict['meta_prints']
        db.content_prints = db_dict['content_prints']

        return db

    def load_meta(self):
        """Loads all metadata (lstats) from the filesystem.

        This should be called once for every database, after creating it and
        before doing anything further with it.
        """
        files = {}
        dirs = {}

        for root, ds, fs in os.walk(self.base):
            rel_root = root[len(self.base)+1:]
            root_meta = DirMeta(root)

            for f in fs:
                full_name = os.path.join(root, f)
                rel_name = full_name[len(self.base)+1:]
                f_meta = FileMeta(full_name)
                files[rel_name] = f_meta
                root_meta.children.append(f_meta)

            dirs[rel_root] = root_meta

        self.files = files
        self.dirs = dirs

    def update_meta(self):
        """Replace the stored metadata with up-to-date info from the
        filesystem."""

        new_meta_prints = {}
        new_content_prints = {}
        for rel_name, file_meta in self.files.iteritems():
            new_meta_prints[rel_name] = file_meta.meta_print

            # if the metadata's the same, assume content hasn't changed either
            if rel_name in self.meta_prints and\
               self.meta_prints[rel_name] == file_meta.meta_print:
                new_content_prints[rel_name] = self.content_prints[rel_name]
            else:
                new_content_prints[rel_name] = file_meta.content_print

        self.meta_prints = new_meta_prints
        self.content_prints = new_content_prints

def delay_filter(delay):
    def _decorator(f):
        last_update = 0

        def _(*args, **kwargs):
            cur = time.time()
            if cur - last_update > delay:
                last_update = cur
                f(*args, **kwargs)
        return _

    return _decorator
