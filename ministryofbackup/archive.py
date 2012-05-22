#!/usr/bin/env python
# coding=utf8

import hashlib
from functools import wraps, partial
from getpass import getpass
from multiprocessing import Process
import os

import logbook
from lzma import LZMACompressor, LZMADecompressor
import M2Crypto
from M2Crypto.m2 import AES_BLOCK_SIZE
from setproctitle import setproctitle

from ministryofbackup.fds import chain_funcs

log = logbook.Logger(__name__)

# the mob header, currently uses the form of 'mobX', where X is the file format
# version
HEADER_LENGTH = 4

# how many bytes of salt to use
SALT_LEN = 32  # 256 bits

# key size (selects cipher)
KEY_BITS = 256
KEY_SIZE = KEY_BITS/8

# number of iterations in pbkdf2
ITERATIONS = 20000

# full openssl cipher string to be used
CIPHER = 'aes_%d_ofb' % KEY_BITS

# buffer size for reading data
DEFAULT_BUFSIZE = 4*1024**2

# random number generator
RNG = os.urandom


def compress(srcfd, destfd, level=9, bufsize=DEFAULT_BUFSIZE):
    setproctitle('mob compression')
    log.debug("Starting compression in process %d" % os.getpid())
    compressor = LZMACompressor(options={'level': level})
    log.debug("Compression level %d" % level)

    src = os.fdopen(srcfd, 'rb')
    dest = os.fdopen(destfd, 'wb')

    while True:
        log.debug('Reading into buffer for compression')
        buf = src.read(bufsize)
        log.debug('Read %d bytes' % len(buf))

        if not buf:
            break
        dest.write(compressor.compress(buf))

    # clean up
    dest.write(compressor.flush())
    log.debug("Compression finished")


def decompress(srcfd, destfd, bufsize=DEFAULT_BUFSIZE):
    setproctitle('mob decompression')
    log.debug("Starting decompression in process %d" % os.getpid())
    decompressor = LZMADecompressor()

    src = os.fdopen(srcfd, 'rb')
    dest = os.fdopen(destfd, 'wb')

    while True:
        buf = src.read(bufsize)
        if not buf:
            break
        dest.write(decompressor.decompress(buf))

    dest.write(decompressor.flush())
    log.debug("Decompression finished")


def encrypt(srcfd, destfd, password, bufsize=DEFAULT_BUFSIZE):
    log.debug("Starting encryption in process %d" % os.getpid())
    setproctitle('mob encryption')
    salt = RNG(SALT_LEN)
    iv = RNG(AES_BLOCK_SIZE)

    key = M2Crypto.EVP.pbkdf2(password, salt, ITERATIONS, KEY_SIZE)

    src = os.fdopen(srcfd, 'rb')
    dest = os.fdopen(destfd, 'wb')

    # write a header for the protocol format
    dest.write('mob1')
    dest.write(salt)
    dest.write(iv)

    aes = M2Crypto.EVP.Cipher(
        alg=CIPHER,
        key=key,
        iv=iv,
        op=M2Crypto.m2.encrypt,
        key_as_bytes=1
    )

    while True:
        log.debug('Reading into buffer for encryption')
        buf = src.read(bufsize)
        if not buf:
            break
        log.debug("Encrypting %d bytes" % len(buf))
        data = aes.update(buf)
        dest.write(data)

    dest.write(aes.final())
    log.debug("Encryption finished")


def decrypt(srcfd, destfd, password, bufsize=DEFAULT_BUFSIZE):
    src = os.fdopen(srcfd, 'rb')
    dest = os.fdopen(destfd, 'wb')

    header = src.read(HEADER_LENGTH)
    if not 'mob1' == header:
        raise Exception('Did not find mob header that I know of. Either you '\
                        'need a newer version of mob or this is no mob file.')
    salt = src.read(SALT_LEN)
    iv = src.read(AES_BLOCK_SIZE)

    key = M2Crypto.EVP.pbkdf2(password, salt, ITERATIONS, KEY_SIZE)

    aes = M2Crypto.EVP.Cipher(
        alg=CIPHER,
        key=key,
        iv=iv,
        op=M2Crypto.m2.decrypt,
        key_as_bytes=1
    )

    while True:
        buf = src.read(bufsize)
        if not buf:
            break
        content = aes.update(buf)
        dest.write(content)

    dest.write(aes.final())


def create_output_chain(srcfd,
                        destfd,
                        password,
                        bufsize=DEFAULT_BUFSIZE,
                        compression_level=9
                       ):

    comp_target = partial(compress, bufsize=bufsize, level=compression_level)
    enc_target = partial(encrypt, password=password, bufsize=bufsize)

    return chain_funcs(srcfd, destfd, [comp_target, enc_target])


def create_input_chain(srcfd,
                       destfd,
                       password,
                       bufsize=DEFAULT_BUFSIZE,
                      ):
    unc_target = partial(decompress, bufsize=bufsize)
    dec_target = partial(decrypt, password=password, bufsize=bufsize)

    return chain_funcs(srcfd, destfd, [dec_target, unc_target])
