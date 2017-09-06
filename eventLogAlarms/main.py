# -*- coding: utf-8 -*-

import gzip
import os
import sys
import xml
import pymongo

class EventAlarm(object):

    def __init__(self):
        self.client = pymongo.MongoClient("localhost", 27017)
        self.db = self.client["eventAlarm"]

    def parseXML(self):
        pass

    def getLogFile(self, file_path):
        file_list = []
        for path, dir, file in os.walk(file_path):
            file_list = [path + "/" + i for i in file]

        return file_list

    def parseLogFile(self, file_list, alarm_info):
        for file in file_list:
            with open(file, "r") as f:
                for temp in f:
                    if temp.startswith("log.game"):
                        # TODO
                        #判断当前日志是否有告警信息
                        pass



if __name__ == '__main__':
    pass
