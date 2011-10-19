#!/usr/bin/env python
# encoding: utf-8
#
# This is the setting file for scribe_log
#

__authors__  = ['jacky wu <jacky.wucheng@gmail.com>', ]
__version__  = 1.0
__date__     = "Feb 16, 2011 3:13:46 PM"
__license__  = "MIT license"

import socket

def gethostname():
    hostname = socket.gethostname()
    return hostname

scribe_config = [
    {
        'file'       : '/data0/log/a.log',
        'category'   : 'a_log',
        'host'       : '1.2.3.4',
        'port'       : 1463,
        'prefix'     : "",
        'postfix'    : " %s" % gethostname(),
    },

    {
        'file'       : '/data0/log/b.log',
        'category'   : 'b_log',
        'host'       : '1.2.3.4',
        'port'       : 1463,
        'prefix'     : "",
        'postfix'    : " %s" % gethostname(),
    },

]
