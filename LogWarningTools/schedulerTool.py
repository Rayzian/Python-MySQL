# -*- coding:utf-8 -*-

"""
author zhouxiaoxi
简乐互动
"""

import time
import pymongo
import traceback
import os
import sys
import Queue
import threading
from multiprocessing import Process
from tools import EventAlarm
from argUtil import genParserClient
from logger import loggerInfo, loggerError


class Scheduler(object):
    def __init__(self):
        self.client = pymongo.MongoClient("localhost", 27017)
        self.db = self.client["existPath"]
        self.collection = self.db["exist"]
        self.q = Queue.Queue()

    def daemonize(self, dir_path, json_dir, ip):
        """
        创建守护进程
        :param dir_path: 日志文件夹路径
        :param save_dir: 保存日志的文件夹路径
        :return: None
        """

        pid = os.fork()
        if pid:
            sys.exit(0)

        os.chdir('%s' % (os.getcwd()))
        os.umask(0)
        os.setsid()

        _pid = os.fork()
        if _pid:
            sys.exit(0)

        sys.stdout.flush()
        sys.stderr.flush()

        loggerInfo("Daemon pid is: %s" % str(os.getpid()))
        with open('/dev/null') as read_null, open('/dev/null', 'w') as write_null:
            os.dup2(read_null.fileno(), sys.stdin.fileno())
            os.dup2(write_null.fileno(), sys.stdout.fileno())
            os.dup2(write_null.fileno(), sys.stderr.fileno())

        while True:
            try:
                for path, dir, files in os.walk(dir_path):
                    if files:
                        for file in files:
                            file_path = os.path.join(path, file)

                            # 检查数据库里是否有该文件路径
                            try:
                                self.collection.find({"path": file_path}).next()
                                continue
                            except StopIteration:
                                loggerInfo("Add %s to the task queue" % str(file))
                                self.q.put(file_path)

                while (not self.q.empty()):
                    file1 = None
                    file2 = None
                    try:
                        file1 = self.q.get(True, timeout=5)
                        loggerInfo("Get file: %s" % file1)
                        file2 = self.q.get(True, timeout=5)
                        loggerInfo("Get file: %s" % file2)
                    except Queue.Empty:
                        loggerInfo("The task queue is empty")

                    if file1 and file2:
                        try:
                            th1 = threading.Thread(target=EventAlarm.startParse, args=(file1, json_dir, ip,), name="thread1")
                            th2 = threading.Thread(target=EventAlarm.startParse, args=(file2, json_dir, ip,), name="thread2")
                            th1.start()
                            time.sleep(1.5)
                            th2.start()
                            th1.join()
                            th2.join()
                        except Exception, e:
                            loggerError(traceback.format_exc())
                        self.collection.insert_one({"path": file1})
                        self.collection.insert_one({"path": file2})
                    else:
                        if file1:
                            th = threading.Thread(target=EventAlarm.startParse, args=(file1, json_dir, ip,),
                                                   name="thread")
                            th.start()
                            th.join()
                            self.collection.insert_one({"path": file1})
                        elif file2:
                            th = threading.Thread(target=EventAlarm.startParse, args=(file2, json_dir, ip,),
                                                   name="thread")
                            th.start()
                            th.join()
                            self.collection.insert_one({"path": file2})

            except Exception, e:
                loggerError(traceback.format_exc())
                loggerInfo('Daemon exits\n')
                cmd = "sudo kill -9 %s" % str(os.getpid())
                os.system(cmd)
                return None


if __name__ == '__main__':
    try:
        # 获取日志文件目录
        config, _ = genParserClient()
        dir_path = config.path
        ip = config.host

        scheduler = Scheduler()

        loggerInfo('Daemon start up')

        # 创建保存日志文件的文件夹
        path = sys.path[0]
        json_dir = os.path.join(path, "result")
        if not os.path.exists(json_dir):
            os.mkdir(json_dir)
        scheduler.daemonize(dir_path=dir_path, json_dir=json_dir, ip=ip)
    except Exception, e:
        print traceback.format_exc()
