#!/usr/bin/env python

from distutils.core import setup

setup(name='nbhttp',
      version='1.0',
      description='Non-blocking HTTP components',
      author='Mark Nottingham',
      author_email='mnot@mnot.net',
      url='http://github.com/mnot/nbhttp/',
      packages=['nbhttp'],
      package_dir={'nbhttp': 'src'},
      scripts=['scripts/proxy.py']
     )