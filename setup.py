from setuptools import setup, find_packages
import sys, os

## use execfile so pip can install deps, _after_ getting version string
libdir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
execfile(os.path.join(libdir, 'pyermc/_version.py'))

setup(
    name="pyermc",
    version=__version__,
    description="python memcache interface",
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
