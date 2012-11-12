from setuptools import setup, find_packages
import sys, os

## use execfile so pip can install deps, _after_ getting version string
libdir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
execfile(os.path.join(libdir, 'pyermc/_version.py'))

setup(
    name = "pyermc",
    version = __version__,
    description = "python memcache interface",
    packages = find_packages(),
    install_requires=[
        'setuptools',
        'umemcache',
        'lz4'
    ],
    zip_safe = False,
)
