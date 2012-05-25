#!/usr/bin/env python
# coding=utf8

from functools import wraps
import multiprocessing
import os
import sys

import logbook

log = logbook.Logger(__name__)

class FileDescriptorRegistry(object):
    _global_instance = None

    def __init__(self, fds=None):
        self.fds = set(fds or [])
        log.debug('New FileDescriptorRegistry created: %s' % hash(self))

    @classmethod
    def get_global_instance(cls):
        if not cls._global_instance:
            cls._global_instance = cls()

        return cls._global_instance

    def add_fd(self, fd):
        if not isinstance(fd, int):
            fd = fd.fileno()

        self.fds.add(fd)
        log.debug('Added %d to FileDescriptorRegistry %s' % (fd, hash(self)))

    def chain_funcs(self, srcfd, destfd, funcs):
        ps = []

        # set up all pipes
        pipe_ends = [srcfd]
        for i in xrange(len(funcs)-1):
            r, w = self.pipe()
            pipe_ends.append(w)
            pipe_ends.append(r)
        pipe_ends.append(destfd)

        log.debug('Setting up process chain, pipe fds: %r' % (pipe_ends,))

        for f in funcs:
            fsrcfd = pipe_ends.pop(0)
            fdestfd = pipe_ends.pop(0)

            target=self.closing_all_except((fsrcfd, fdestfd))(f)

            p = multiprocessing.Process(target=target, args=(fsrcfd, fdestfd))
            p.daemon = True
            p.start()
            log.debug('Started process [%d], srcfd: %d, destfd: %d' % (
                p.pid, fsrcfd, fdestfd
            ))

            ps.append(p)

        return ps

    def close(self, fd):
        self.fds.remove(fd)

        return os.close(fd)

    def close_all_except(self, keep_open=[]):
        keep_open = set(keep_open)
        pid = os.getpid()
        new_fds = []
        for fd in self.fds:
            if fd in keep_open:
                log.debug(
                    'Keeping file descriptor %d open in process %d' % (fd, pid)
                )
                new_fds.append(fd)
            else:
                log.debug(
                    'Closing file descriptor %d in process %d' % (fd, pid)
                )
                try:
                    os.close(fd)
                except OSError, e:
                    log.debug('Error closing %d, ignored: %s' % (fd, e))

        self.fds = set(new_fds)

    def closing_all_except(self, keep_open=[]):
        def wrapper(f):
            def _(*args, **kwargs):
                self.close_all_except(keep_open)
                return f(*args, **kwargs)
            return _

        return wrapper

    def open(self, *args, **kwargs):
        fd = os.open(*args, **kwargs)
        self.fds.add(fd)

        return fd

    def pipe(self):
        pipe_fds = os.pipe()
        self.fds.update(pipe_fds)
        log.debug('Created new pipe: r=%d, w=%d' % pipe_fds)

        return pipe_fds

    def __repr__(self):
        return 'FileRegistry(%r)' % self.fds


