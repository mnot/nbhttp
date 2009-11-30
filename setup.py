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
      classifiers=[
           'Development Status :: 4 - Beta',
           'Intended Audience :: Developers',
           'License :: OSI Approved :: MIT License',
           'Programming Language :: Python',
           'Topic :: Internet :: WWW/HTTP', 
           'Topic :: Internet :: Proxy Servers',
           'Topic :: Internet :: WWW/HTTP :: HTTP Servers',
           'Topic :: Software Development :: Libraries :: Python Modules',
      ]
     )
