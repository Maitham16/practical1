-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA512

Format: 3.0 (quilt)
Source: python3.7
Binary: python3.7, python3.7-venv, libpython3.7-stdlib, python3.7-minimal, libpython3.7-minimal, libpython3.7, python3.7-examples, python3.7-dev, libpython3.7-dev, libpython3.7-testsuite, idle-python3.7, python3.7-doc, python3.7-dbg, libpython3.7-dbg
Architecture: any all
Version: 3.7.3-2+deb10u5
Maintainer: Matthias Klose <doko@debian.org>
Standards-Version: 4.3.0
Vcs-Browser: https://salsa.debian.org/cpython-team/python3/tree/python3.7
Vcs-Git: https://salsa.debian.org/cpython-team/python3.git -b python3.7
Testsuite: autopkgtest
Testsuite-Triggers: build-essential, gdb, locales-all, python3-distutils, python3-gdbm, python3-gdbm-dbg, python3-tk, python3-tk-dbg
Build-Depends: debhelper (>= 9), dpkg-dev (>= 1.17.11), quilt, autoconf, lsb-release, sharutils, libreadline-dev, libncursesw5-dev (>= 5.3), zlib1g-dev, libbz2-dev, liblzma-dev, libgdbm-dev, libdb-dev, tk-dev, blt-dev (>= 2.4z), libssl-dev, libexpat1-dev, libmpdec-dev (>= 2.4), libbluetooth-dev [linux-any] <!pkg.python3.7.nobluetooth>, locales-all, libsqlite3-dev, libffi-dev (>= 3.0.5) [!or1k !avr32], libgpm2 [!hurd-i386 !kfreebsd-i386 !kfreebsd-amd64], mime-support, netbase, bzip2, time, python3:any, net-tools, xvfb, xauth
Build-Depends-Indep: python3-sphinx, texinfo
Package-List:
 idle-python3.7 deb python optional arch=all
 libpython3.7 deb libs optional arch=any
 libpython3.7-dbg deb debug optional arch=any
 libpython3.7-dev deb libdevel optional arch=any
 libpython3.7-minimal deb python optional arch=any
 libpython3.7-stdlib deb python optional arch=any
 libpython3.7-testsuite deb libdevel optional arch=all
 python3.7 deb python optional arch=any
 python3.7-dbg deb debug optional arch=any
 python3.7-dev deb python optional arch=any
 python3.7-doc deb doc optional arch=all
 python3.7-examples deb python optional arch=all
 python3.7-minimal deb python optional arch=any
 python3.7-venv deb python optional arch=any
Checksums-Sha1:
 e3584650a06ae2765da0678176deae9d133f1b3d 17108364 python3.7_3.7.3.orig.tar.xz
 2bce3e56d3467da122d8ac6fef55a1002632eba4 240848 python3.7_3.7.3-2+deb10u5.debian.tar.xz
Checksums-Sha256:
 da60b54064d4cfcd9c26576f6df2690e62085123826cff2e667e72a91952d318 17108364 python3.7_3.7.3.orig.tar.xz
 c01dc42ccca8f1fa0087dfaba9347e31c163e8a147c45dda81172869887945c3 240848 python3.7_3.7.3-2+deb10u5.debian.tar.xz
Files:
 93df27aec0cd18d6d42173e601ffbbfd 17108364 python3.7_3.7.3.orig.tar.xz
 3ace96d9e70b03dc0279f94969b799f7 240848 python3.7_3.7.3-2+deb10u5.debian.tar.xz

-----BEGIN PGP SIGNATURE-----

iQIzBAEBCgAdFiEEOvp1f6xuoR0v9F3wiNJCh6LYmLEFAmSeCq8ACgkQiNJCh6LY
mLFo6A//Wkmu+Le/DioCYHapR4cPeeKZZTF6kEeOolebdUfRKPeZTwrX45S9EZiP
7qeiIPyHXM+2qtUb/eaUx2RfbDsLdf+GIUOC6JFTvwx58/6Dlymgei2oFONXXCvD
UfjaHicHXKe0HQSZqpn/xErNkQcmMafjvYaShkNsRIatdVjyR/nSqKOatvGHTDMj
lJUJ3DmmYO3sT4P1WZ10hLFBB/1/QJfiuFoqgmD0HX+TXq8fEvYYNSsw3nD4m761
k9qhc9RebXCEhgSTOoMBODlg1iKE5zFMv9HQAN5kZZnTEnR+sWcWW8SjAOWU/sbP
nZqSzW5bzWj4CFBvn0fpq2qaT7bGeOk000mBJkrZJ0EvxsXxEMOa6gIi7SKXW2QE
3EDlc2COkTHEGTvwuGYri3oynXpb4b0RQRDyIok08Ecfztwn+3MC7FgliWskGsmm
GazK8H+sboHmGSO8jdwrKt/tRLhZg27EbQyAY2Y92gRFNyPjBwulGIvcDpJdJQ9C
EcaKsYKkSk2wHekJxfZVMTVtTwlWjIEl7P9k2TD1bDath3w7kQS/b79kk/2RunqV
6cP1kGTrM1wlfMhvJpu+XLv3Zac3JKLTlwYxiS5OyZ3QicTf6PAHQI/GXmJ0Z8b0
Sr3dRdlUA0blkYwGdfg7DShk/C1pa/NcAXOpDGL1ulsbq52Jpn0=
=xSbg
-----END PGP SIGNATURE-----
