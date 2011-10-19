#!/usr/bin/env python
# encoding: utf-8
#
# This script send log data to Scribed, Reload file if realpath changes,
# inode changes or if the file is truncated. The author is silas and ben,
# thanks for their work. I add multithreading and exception deal to it.
from optparse import OptionParser
from scribe import scribe
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
import daemon
import logging
import logging.handlers
import os
import setting
import threading
import time

__authors__  = ['jacky wu <wucheng@staff.sina.com.cn>', ]
__version__  = 1.0
__date__     = "Feb 16, 2011 3:13:46 PM"
__license__  = "MIT license"


class Logging(object):
    def __init__(self, log_path ,logger_name, back_count=5, max_bytes=1000000000):
        """
        Args:
            log_path: where the log is put
            logger_name: name of the logger
            back_count: how many old log will be kept
            max_bytes: exceed the max size will trigger rotate the log file
        """
        log_dir = os.path.dirname(log_path)
        if not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except os.error, e:
                raise Exception("Create scatter log dir <%s> fail: %s" % (log_dir,e))
            
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        
        self.file_handler = logging.handlers.RotatingFileHandler(log_path, 'a', max_bytes, back_count)
        self.file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.file_handler.setFormatter(formatter)
        self.logger.addHandler(self.file_handler)

    def get_logger(self):
        return self.logger
    
    def __del__(self):
        self.file_handler.close()
#===============================================================================
LOG_PATH = './log/scribe.log'
LOGGER_NAME = 'Scribe'
LOGGER_OBJ = Logging(LOG_PATH, LOGGER_NAME)
G_LOGGER = LOGGER_OBJ.get_logger()
#===============================================================================

class Error(Exception): pass
class FileError(Error): pass

class Tail(object):

    def __init__(self, path, sleep=1.0, reopen_count=31536000):
        self.path = path
        self.sleep = sleep
        self.reopen_count = reopen_count

    def __iter__(self):
        while True:
            pos = self.file.tell()
            line = self.file.readline()
            if not line:
                self.wait(pos)
            else:
                yield line

    def open(self, tail=False):
        """open the file specified by path.

          Args:
            tail: False -> lseek to the begin of the file
                  True -> lseek to the end of the file
        """
        try:
            self.real_path = os.path.realpath(self.path)
            self.inode = os.stat(self.path).st_ino
        except OSError, error:
            raise FileError(error)
        try:
            self.file = open(self.real_path)
        except IOError, error:
            raise FileError(error)
        if tail:
            self.file.seek(0, 2) # seek to end of file

    def close(self):
        try:
            self.file.close()
        except Exception:
            pass

    def reopen(self):
        """ reopen the file, and if error occured between open procedure exceed
        <reopen_count> times, return False to present reopen file failed.
        """
        self.close()
        reopen_count = self.reopen_count
        while reopen_count >= 0:
            reopen_count -= 1
            try:
                self.open(tail=False) # open a new file and seek at the header
                return True
            except FileError:
                time.sleep(self.sleep)
        return False

    def check(self, pos):
        """ if return True, it said the file has been moved, and Tail should
        point to a new file.
        """
        try:
            if self.real_path != os.path.realpath(self.path):
                return True
            stat = os.stat(self.path)
            if self.inode != stat.st_ino:
                return True
            if pos > stat.st_size:
                return True
        except OSError:
            return True
        return False

    def wait(self, pos):
        if self.check(pos): # file has been moved, need to reopen file
            if not self.reopen():
                raise Error('Unable to reopen file: %s' % self.path)
        else: # no need to reopen it, just seek back to <pos>
            self.file.seek(pos)
            time.sleep(self.sleep)

def scribe_fix_legacy():
    global scribe
    old_log_entry = scribe.LogEntry
    def new_log_entry(**kwargs):
        return old_log_entry(kwargs)
    scribe.LogEntry = new_log_entry

class Handler(threading.Thread):
    def __init__(self, path, category, host='127.0.0.1', port=1463, 
                 prefix='', postfix='', tail=True):
        threading.Thread.__init__(self)
        self.__path = path
        self.__category = category
        self.__host = host
        self.__port = port
        self.__prefix = prefix
        self.__postfix = postfix
        self.__tail = tail
    
    def __connect(self):
        self.__socket = TSocket.TSocket(host=self.__host, port=self.__port)
        self.__transport = TTransport.TFramedTransport(self.__socket)
        self.__protocol = TBinaryProtocol.TBinaryProtocol(
            trans=self.__transport,
            strictRead=False,
            strictWrite=False,
        )
        self.__client = scribe.Client(iprot=self.__protocol, oprot=self.__protocol)
        self.__transport.open()
    
    def run(self):
        result = 0
    
        self.__connect()
        
        try:    
            tail = Tail(self.__path)
            try:
                tail.open(self.__tail)
                for line in tail:
                    try:
                        log_entry = scribe.LogEntry(
                            category=self.__category,
                            message="%s%s%s\n" % (self.__prefix, line.strip(), self.__postfix)
                        )
                    except TypeError:
                        scribe_fix_legacy()
                        log_entry = scribe.LogEntry(
                            category=self.__category,
                            message="%s%s%s\n" % (self.__prefix, line.strip(), self.__postfix)
                        )
                        
                    while True:
                        try:
                            result = self.__client.Log(messages=[log_entry])
                            break
                        except Exception, e:
                            while True:
                                G_LOGGER.exception(e)
                                sleep_interval = 1
                                time.sleep(sleep_interval)
                                try:
                                    self.__connect()
                                    break
                                except Exception,e:
                                    G_LOGGER.exception(e)
                                    time.sleep(sleep_interval)
                            
                        
            finally:
                tail.close()
        finally:
            try:
                self.__transport.close()
            except Exception:
                pass
    
        if result == scribe.ResultCode.OK:
            pass
        elif result == scribe.ResultCode.TRY_LATER:
            raise Error('Scribe Error: TRY LATER')
        else:
            raise Error('Scribe Error: Unknown error code (%s)' % result)

def main():
    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage=usage)
    parser.add_option("-t", "--tail", dest="tail", action="store_false", default=True, 
                      help="if send file content from the end of it")
    (options, args) = parser.parse_args()
    
    try:
        pid_file = '/var/run/scribe_log.pid'
        daemon.daemonize(pid_file)
        
        threads = []
        for task in setting.scribe_config:
            threads.append(Handler(task['file'],
                                   task['category'],
                                   task['host'],
                                   task['port'],
                                   task['prefix'],
                                   task['postfix'],
                                   options.tail))

        for t in threads:
            t.setDaemon(True)
            t.start()
    
        for t in threads:
            t.join()
    except Exception,e:
        G_LOGGER.exception(e)

if __name__ == '__main__':
    main()