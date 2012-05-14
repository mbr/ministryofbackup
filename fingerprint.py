#!/usr/bin/env python
# coding=utf8

from binascii import hexlify
from hashlib import sha1
import mmap
import os
import stat
import sys
import time

import msgpack
import progressbar
from remember.memoize import memoize, memoized_property

DATA_PROGRESS_BAR = ['Complete: ', progressbar.Percentage(), ' ',
                        progressbar.Bar(marker='#', left='[', right=']'),
                        ' ', progressbar.ETA(), ' ',
                        progressbar.FileTransferSpeed()]

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


class FileMeta(MetaBase):
    read_buf_size = 1024*1024*4  # 4M should be sufficient for speed and not
                                 # too memory hungry

    @memoized_property
    def content_print(self):
        """Content prints rely only on the contents of the file - pretty much a
        'normal' application of the underlying hash function"""
        ftype = stat.S_IFMT(self.s.st_mode)

        if stat.S_IFLNK == ftype:
            return ''
        elif stat.S_IFREG == ftype:
            h = sha1()
            remain = self.get_filesize()

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


class DirMeta(MetaBase):
    pass


class Database(object):
    def __init__(self, base):
        self.base = base
        self.meta_prints = {}
        self.content_prints = {}

    def dump(self, outfile):
        db_dict = {
            'meta_prints': self.meta_prints,
            'content_prints': self.content_prints
        }
        msgpack.dump(db_dict, f)

    def get_altered_files(self, fileset=None, progress=None):
        fileset = fileset or self.files.keys()

        altered = []

        n_bytes = 0
        for rel_name in fileset:
            f = self.files[rel_name]
            if rel_name in self.content_prints:
                if f.content_print != self.content_print[rel_name]:
                    altered.append(rel_name)

            if progress:
                n_bytes += f.filesize
                progress(n_bytes)

        return altered

    def get_deleted_files(self):
        return [rel_name for rel_name in self.meta_prints.iterkeys() if rel_name
        not in self.files]

    def get_new_and_updated_files(self, progress=None):
        # determine which files have changed
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
            if self.meta_prints[rel_name] != file_meta.meta_prints:
                updated.append(rel_name)

        return new, updated

    def get_sizes_of(self, fileset):
        return sum(self.files[rel_name].filesize for rel_name in fileset)

    @classmethod
    def load(cls, base, infile):
        db_dict = msgpack.load(infile)

        db = cls(base)
        db.meta_prints = db_dict['meta_prints']
        db.content_prints = db_dict['content_prints']

        return db

    def load_meta(self):
        files = {}
        dirs = {}

        for root, ds, fs in os.walk(base):
            rel_root = root[len(base)+1:]
            root_meta = DirMeta(root)

            for f in fs:
                full_name = os.path.join(root, f)
                rel_name = full_name[len(base)+1:]
                f_meta = FileMeta(full_name)
                files[rel_name] = f_meta
                root_meta.children.append(f_meta)

            dirs[rel_root] = root_meta

        self.files = files
        self.dirs = dirs

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

if '__main__' == __name__:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    parser.add_argument('--db', default='fingerprints.db')
    args = parser.parse_args()

    base = os.path.abspath(args.directory)
    print "Base directory: %s" % base

    if os.path.exists(args.db):
        print "Loading fingerprint database '%s'" % args.db
        with open(args.db, 'rb') as f:
            db = Database.load(base, f)
    else:
        print "New fingerprint database"
        db = Database(base)

    # collect filenames on filesystem
    db.load_meta()

    print "Collected %d files in %d directories" % (len(db.files),
                                                    len(db.dirs))

    new, updated = db.get_new_and_updated_files()

    # altered file checks with progress-bar
    altered = []
    if updated:  # could also force checking all files here with cmdline arg?
        pbar = progressbar.ProgressBar(widgets=DATA_PROGRESS_BAR,
                                       maxval=db.get_sizes_of(updated))
        pbar.start()
        altered = db.get_altered_files(updated, progress=pbar.progress)
        pbar.finish()
    deleted = db.get_deleted_files()

    print "Found %d new files, %d updated, %d altered and %d deleted files" % (
        len(new), len(updated), len(altered), len(deleted)
    )

    sys.exit(0)

    # FIXME: properly handle transition from old meta to new meta
    print "Writing to database"
    db = {
        'meta_prints': meta_prints,
        'content_prints': content_prints
    }

    sys.exit(0)

    with open(args.db, 'wb') as f:
        msgpack.dump(db, f)
