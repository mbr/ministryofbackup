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

PROGRESS_BAR_WIDGETS = ['Complete: ', progressbar.Percentage(), ' ',
                        progressbar.Bar(marker='#', left='[', right=']'),
                        ' ', progressbar.ETA(), ' ',
                        progressbar.FileTransferSpeed()]

# queue based: process directory by queue'ing its children
class FileMeta(object):
    read_buf_size = 1024*1024*4

    def __init__(self, filename):
        self.filename = filename

    def _get_stats(self):
        if not hasattr(self, 's'):
            self.update_stats()

    def get_content_print(self):
        """Content prints rely only on the contents of the file - pretty much a
        'normal' application of the underlying hash function"""

        self._get_stats()

        ftype = stat.S_IFMT(self.s.st_mode)

        if stat.S_IFLNK == ftype:
            return ''
        elif stat.S_IFREG == ftype:
            h = sha1()
            remain = self.get_filesize()

            with open(self.filename, 'rb') as src:
                buf = True
                while remain:
                    buf = src.read(min(self.read_buf_size, remain))
                    remain -= len(buf)
                    h.update(buf)

            return h.digest()
        else:
            raise Exception('Cannot handle filetype %s - sorry' % ftype)

    def get_filesize(self):
        self._get_stats()
        return self.s.st_size

    def get_meta_print(self):
        """The meta print is a fingerprint based solely on the metadata of the
        file, not the contents"""

        self._get_stats()
        stat_string = ' '.join(map(str, iter(self.s)))

        return sha1(stat_string).digest()

    def update_stats(self):
        self.s = os.lstat(self.filename)
        return self.s

    def __repr__(self):
        return 'FM<%r>' % self.filename


if '__main__' == __name__:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('directory')
    parser.add_argument('--db', default='fingerprints.db')
    args = parser.parse_args()

    # read database
    if os.path.exists(args.db):
        print "Loading fingerprint database '%s'" % args.db
        with open(args.db, 'rb') as f:
            old_db = msgpack.load(f)

    # collect filenames
    base = os.path.abspath(args.directory)
    file_metas = {}
    dirnames = []

    for root, dirs, files in os.walk(base):
        for f in files:
            full_name = os.path.join(root, f)
            rel_name = full_name[len(base)+1:]
            file_metas[rel_name] = FileMeta(full_name)

        for d in dirs:
            dn = os.path.join(root, d)[len(base):]
            dirnames.append(dn)

    print "Collected %d files in %d directories" % (len(file_metas),
                                                    len(dirnames))

    print "Getting meta prints"
    meta_prints = {name : fm.get_meta_print() for name, fm in
                   file_metas.iteritems()}

    # determine which files have changed
    new_files = []
    updated_meta_files = []
    altered_files = []
    if old_db:
        print "Looking for changed files"
        for fn, meta_print in meta_prints.iteritems():
            if not fn in old_db['meta_prints']:
                new_files.append(fn)
            elif old_db['meta_prints'][fn] != meta_print:
                updated_meta_files.append(fn)
    else:
        raise Exception('FIXME')

    print "Found %d new files, checking %d old files for changes" % (
        len(new_files), len(updated_meta_files)
    )

    candidates = [file_metas[name] for name in updated_meta_files]

    def collect_content_prints(fms, cb=None):
        content_prints = {}

        last_update = time.time()
        n_bytes = 0
        n_files = 0

        for fm in fms:
            cur_time = time.time()

            if cb and cur_time - last_update > 1:
                last_update = cur_time
                cb(n_bytes, n_files)

            content_prints[fm.filename] = fm.get_content_print()
            n_bytes += fm.get_filesize()
            n_files += 1

        return content_prints

    file_sizes_sum = sum(fm.get_filesize() for fm in file_metas.itervalues())
    pbar = progressbar.ProgressBar(widgets=PROGRESS_BAR_WIDGETS,
                                   maxval=file_sizes_sum)
    pbar.start()
    content_prints = collect_content_prints(candidates,
                                            lambda n,_: pbar.udpate(n))
    pbar.finish()

    # see which files have changed and which haven't
    for name, content_print in content_prints.iteritems():
        if old_db['content_prints'][name] != content_print:
            del updated_meta_files[name]
            altered_files.append(name)

    print "NEW", new_files
    print "META", updated_meta_files
    print "ALTERED", altered_files

    print "Writing to database"
    db = {
        'meta_prints': meta_prints,
        'content_prints': content_prints
    }

    sys.exit(0)

    with open(args.db, 'wb') as f:
        msgpack.dump(db, f)
