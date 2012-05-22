#!/usr/bin/env python
# coding=utf8

import os

import logbook

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
