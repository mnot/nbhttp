#!/usr/bin/env python

from distutils.core import setup

setup(name='nbhttp',
      version='0.7.2',
      description='Non-blocking HTTP components',
      author='Mark Nottingham',
      author_email='mnot@mnot.net',
      url='http://github.com/mnot/nbhttp/',
      download_url='http://github.com/mnot/nbhttp/tarball/nbhttp-0.7.2',
      packages=['nbhttp'],
      package_dir={'nbhttp': 'src'},
      scripts=['scripts/proxy.py'],
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