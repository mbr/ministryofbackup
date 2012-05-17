#!/usr/bin/env python
# coding=utf8

import hashlib
from getpass import getpass
from lzma import LZMACompressor, LZMADecompressor
from multiprocessing import Process
import os
from setproctitle import setproctitle

import logbook
import M2Crypto
from M2Crypto.m2 import AES_BLOCK_SIZE

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
    log.debug("Compressiong level %d" % level)

    src = os.fdopen(srcfd, 'rb')
    dest = os.fdopen(destfd, 'wb')

    while True:
        buf = src.read(bufsize)
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
        buf = src.read(bufsize)
        if not buf:
            break
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


def create_output_chain(inputfd,
                        outputfd,
                        password,
                        bufsize=DEFAULT_BUFSIZE,
                        compression_level=9
                       ):
    log.debug('Setting up output chain from process %d' % os.getpid())

    encrypt_fdr, encrypt_fdw = os.pipe()

    # compression
    comp_p = Process(target=compress, kwargs={
        'srcfd': inputfd,
        'destfd': encrypt_fdw,
        'level': compression_level,
        'bufsize': bufsize,
    })

    comp_p.daemon = True
    comp_p.start()

    # close open fd, so encrypt doesn't hang
    os.close(encrypt_fdw)

    enc_p = Process(target=encrypt, kwargs={
        'srcfd': encrypt_fdr,
        'destfd': outputfd,
        'password': password,
        'bufsize': bufsize,
    })

    enc_p.daemon = True
    enc_p.start()

    return [comp_p, enc_p]


def create_input_chain(inputfd,
                       outputfd,
                       password,
                       bufsize=DEFAULT_BUFSIZE,
                      ):
    log.debug('Setting up input chain from process %d' % os.getpid())

    compress_fdr, compress_fdw = os.pipe()

    # decryption
    dec_p = Process(target=decrypt, kwargs={
        'srcfd': inputfd,
        'destfd': compress_fdw,
        'password': password,
        'bufsize': bufsize,
    })

    dec_p.daemon = True
    dec_p.start()

    os.close(compress_fdw)

    unc_p = Process(target=decompress, kwargs={
        'srcfd': compress_fdr,
        'destfd': outputfd,
        'bufsize': bufsize,
    })

    unc_p.daemon = True
    unc_p.start()

    return [dec_p, unc_p]


if '__main__' == __name__:
    import argparse
    import sys
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('store', 'restore'))
    parser.add_argument('-p', '--password', default=None)
    parser.add_argument('-b', '--bufsize', default=DEFAULT_BUFSIZE, type=int)
    parser.add_argument('-c', '--compression-level', type=int, default=9,
                              choices=range(10))
    parser.add_argument('-d', '--debug',
                               action='append_const',
                               const=logbook.DEBUG,
                               dest='loglevel')
    parser.add_argument('-v', '--verbose',
                              action='append_const',
                              const=logbook.INFO,
                              dest='loglevel')
    parser.add_argument('-i', '--infile',
                        type=argparse.FileType('rb'),
                        default=sys.stdin)
    parser.add_argument('-o', '--outfile',
                        type=argparse.FileType('wb'),
                        default=sys.stdout)

    args = parser.parse_args()

    loglevel = min(args.loglevel) if args.loglevel else logbook.NOTICE

    logbook.NullHandler().push_application()
    logbook.StderrHandler(level=loglevel).push_application()

    try:
        if None == args.password:
            password = getpass('Enter archive password: ')
        else:
            password = args.password

        start_time = time.time()
        if 'store' == args.action:
            log.info('Compressing and encrypting %s' % args.infile.name)
            ps = create_output_chain(args.infile.fileno(),
                                     args.outfile.fileno(),
                                     password=password,
                                     bufsize=args.bufsize,
                                     compression_level=args.compression_level)
        elif 'restore' == args.action:
            log.info('Decrypting and decompressing %s' % args.infile.name)
            ps = create_input_chain(args.infile.fileno(),
                                    args.outfile.fileno(),
                                    password=password,
                                    bufsize=args.bufsize,
                                    )

        # wait for processes to end
        for p in ps:
            p.join()
        end_time = time.time()

        log.info('Done after %.1f seconds' % (end_time-start_time))
    except Exception, e:
        if loglevel <= logbook.DEBUG:
            log.exception(e)
        else:
            log.error(e)
        sys.exit(1)
