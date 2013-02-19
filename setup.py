from setuptools import setup, find_packages
import sys, os

## use execfile so pip can install deps, _after_ getting version string
libdir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
execfile(os.path.join(libdir, 'pyermc/_version.py'))

long_description = """
pyermc is a memcache client library with support for "pluggable"
driver backends.

Current backends include a text protocol driver (default), a binary
protocol driver, and a wrapped ultramemcached driver.

While pyermc is somewhat compatible with python-memcached, full
compatibility is not a design goal.

pyermc supports connecting to a single memcached instance (or server
that speaks the memcached protocol). If you require multiple servers,
consistent hashing, etc, then it is recommended to use twemproxy or
similar.

pyermc exposes connectivity faults, via exceptions, to the calling
code. This is in contrast to python-memcached, which simply enters an
'ignore backend' state. Exposing underlying faults is often needed when
working with queueing servers that support the memcache driver (such as
kestrel and darner), as well as translation proxies like couchbase's
moxie. If the old behavior is desired, there is an optional error_as_miss
parameter that may be set as part of client creation.
""".strip()


setup(
    name="pyermc",
    version=__version__,
    description="python memcache interface",
    long_description=long_description,
    packages=find_packages(),
    install_requires=[
        'setuptools',
        'lz4'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
    ],
    extras_require={
        'umemcache_driver': ['umemcache']
    },
    zip_safe=False,
)
