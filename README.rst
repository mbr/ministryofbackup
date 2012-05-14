Ministry of Backup
==================
Don't you hate long README's? =)

Motivation
----------
*Skip this section if you like!*

Recently, I've tried to create backups of my personal files. A key feature was
storing them "in the cloud", that is, on `amazon S3`_, as I do not want to spend
any time maintaining the hardware - and having offsite backups is a plus.
Encryption was mandatory, but this can be handled through `encfs
<http://www.arg0.net/encfs>`_ if not
available, so this will not be a concern in the following.

My personal documents at that time weighed in at 47G, spread over a litte over
110,000 files in 20,000 directories. The most comfortable approach seemed to be
`Dropbox <http://dropbox.com>`_, but the version I tried choked heavily even
trying to index a folder this large. In short, I did not want to wait about 1-2
weeks for it finish backup up, if at all.

A number of other solutions are available, to prominent candidates are `s3fs
(FuseOverAmazon) <http://code.google.com/p/s3fs/wiki/FuseOverAmazon>`_ and
`s3backer <https://code.google.com/p/s3backer/>`_. These are really cool tools,
however, in both cases they result in a lot of small files to be created
uploaded, which S3 doesn't do very well. The overhead from the HTTP-based API
essentially kills this. Both tools also did not go to much length to give me
any idea what was going on and the frequent pauses in uploading are enough to
drive me mad.

At the conclusion that only incremental backups with archives can be a good
solution, I gave `duplicity <http://duplicity.nongnu.org/>`_. I tried it with
and without the nice-looking `Déjà Dup <http://live.gnome.org/DejaDup>`_
frontend, but the result were horrible: Either support for the Ireland region
of S3 was bad, in the end, the whole tool crashed a little too often for me to
feel secure and offered no obvious way of simply *extracting* (not restoring!)
backups should things go wrong. The whole "S3 backend" (one among many) seems
rather like an afterthought and badly implemented. I decided I'd rather not
trust it with my data and write a specialized solution to fit my needs.

I'm not prone to NIH - if you can propose a fast, efficient and clean backup
solution that allows me store data on S3, send me a message.

Design principles
-----------------
The Ministry of Backup, short: *mob*, is a tool that keeps incremental backups
of a folder on `amazon S3`_. The backups need to be:

* encrypted: Encryption should be done by an established tool or library
somewhat restorable
* restorable without mob: At most 30-60 minutes of writing a small worst-case
tool should be able to get the data back even when mob is not available.
* reasonably space-efficient: Ideally, we do not waste any space. Some things
that are just too hard/cumbersome (diff on files) can be left out.
* robust: The actions taken by the tool should be as atomic as possible and
leave the internal database or backup always in a valid state. No crash in the
middle of backing up should ever pose a problem.
* calming: mob should always display its current progress and try hard to to
resume and speed up things when possible
* simple: The problem is defined narrowly and this should reflect in the code.
A premium is placed on its simplicity, as this makes writing a solid tool
easier.

For this to work, some assumptions about the machine running mob are made.
These allow simplification of the design:

* I/O bound: The computer is no supercomputer, but fast. As long as operations
keep within the same Big-O set, they are still fast enough. I/O operations need
to be as fast as possible and may never be wasteful. Example: It's okay to use
SHA1 as a hash-algorithm for a file, as it will never max out CPU usage on my
machine even when hashing from SSD.
* Plenty of memory: We are not working with a heavily memory restrained
computer. It's okay to keep the whole file list in memory and maybe even make a
copy of it occasionally.
* Big uplink (at least for a home machine): There is plenty of upload bandwidth
(upwards of 1M/s) available and some measures should be taken to max these as
much as possible.
* Knowledgeable user: It's okay to have a few more complex requirements, as the
administrator of the machine can set things up.

Requirements
------------
In its current iteration, mob requires the following things to be given:

* No live changes: The directory backed up must not have any changes made to
itself or any of its subdirectories/files while the program is running. This
can be achieved by using snapshots on some filesystems (`btrfs`_
, `XFS
<http://en.wikipedia.org/wiki/XFS>`_) or simply not touching the directory
while doing  backups.

Features to think about in the futures
--------------------------------------
* single-file diffs: When using snapshots, maybe keep the previous snapshot
(possible on `COW <http://en.wikipedia.org/wiki/Copy-on-write>`_-filesystems
like `btrfs`_) to calculate diffs and store these.

.. _amazon S3: http://aws.amazon.com/s3/

.. _btrfs: http://en.wikipedia.org/wiki/Btrfs
