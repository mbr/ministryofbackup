#!/usr/bin/env python
# coding=utf8

from functools import wraps
import multiprocessing
import os
import sys

import logbook

log = logbook.Logger(__name__)

class FileDescriptorRegistry(object):
    def __init__(self, fds=None):
        self.fds = fds or []

    def add_fd(self, fd):
        self.fds.append(fd)

    def close(self, fd):
        self.fds.remove(fd)

        return os.close(fd)

    def close_fds(self, keep_open=[]):
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
                os.close(fd)

        self.fds = new_fds

    def closing_fds_except(self, keep_open=[]):
        def wrapper(f):
            def _(*args, **kwargs):
                self.close_fds(keep_open)
                return f(*args, **kwargs)
            return _

        return wrapper

    def open(self, *args, **kwargs):
        fd = os.open(*args, **kwargs)
        self.fds.append(fd)

        return fd

    def pipe(self):
        pipe_fds = os.pipe()
        self.fds.extend(pipe_fds)

        return pipe_fds


def chain_funcs(srcfd, destfd, funcs):
    ps = []
    fdreg = FileDescriptorRegistry([srcfd, destfd])

    # set up all pipes
    pipe_ends = [srcfd]
    for i in xrange(len(funcs)-1):
        r, w = fdreg.pipe()
        pipe_ends.append(w)
        pipe_ends.append(r)
    pipe_ends.append(destfd)

    log.debug('Setting up process chain, pipe fds: %r' % (pipe_ends,))

    for f in funcs:
        fsrcfd = pipe_ends.pop(0)
        fdestfd = pipe_ends.pop(0)

        target=fdreg.closing_fds_except((fsrcfd, fdestfd))(f)

        p = multiprocessing.Process(target=target, args=(fsrcfd, fdestfd))
        p.daemon = True
        p.start()
        log.debug('Started process [%d], srcfd: %d, destfd: %d' % (
            p.pid, fsrcfd, fdestfd
        ))

        ps.append(p)

    # close all pipe ends created
    fdreg.close_fds(keep_open=(srcfd, destfd))

    return ps
