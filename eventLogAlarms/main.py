# -*- coding: utf-8 -*-

import gzip
import requests
import json
import os
import re
import sys
from xml.dom.minidom import parse
import xml.dom.minidom
import pymongo
from argUtil import genParserClient


class _GZipTool(object):
    def __init__(self, bufSize):
        self.bufSize = bufSize
        self.fin = None
        self.fout = None

    def decompress(self, gzFile, dst):
        print "start to decompress: ", gzFile
        self.fin = gzip.open(gzFile, 'rb')
        self.fout = open(dst, 'wb')
        self.__in2out()
        print "decompress {} final.".format(gzFile)
        return dst

    def __in2out(self):
        while True:
            buf = self.fin.read(self.bufSize)
            if len(buf) < 1:
                break
            self.fout.write(buf)
        self.fin.close()
        self.fout.close()


class EventAlarm(object):
    def __init__(self):
        self.client = pymongo.MongoClient("localhost", 27017)
        self.db = self.client["eventAlarm"]

    def parseXML(self):
        xml_dict = {}

        DOMTree = xml.dom.minidom.parse("index.xml")
        Data = DOMTree.documentElement
        catalogs = Data.getElementsByTagName("logName")
        for catalog in catalogs:
            if catalog.hasAttribute("filterkey"):
                file_name = catalog.getAttribute("filterkey")
                xml_dict[file_name] = {}

            for key in catalog.getElementsByTagName("filed"):
                id = key.getAttribute("name")
                value = key.getAttribute("limit")
                xml_dict[file_name][id] = value

        if xml_dict:
            return xml_dict

    def parseLogFile(self, file_path, xml):
        path, flag = self.checkFileType(path=file_path)
        with open(path, "r") as f:
            for temp in f:
                # filterkey = xml.keys()[0]
                for filterkey in xml.keys():
                    if temp and str(filterkey) in temp:
                        if str(filterkey) != "wupin":
                            for key in xml[filterkey].keys():
                                transkey = str(key) + ":"
                                if transkey == "yinbi:":
                                    index = r'yinbi:(\S+)'
                                    pattern = re.compile(pattern=index)
                                else:
                                    index = r'%s:(\S+?);' % str(key)
                                    pattern = re.compile(pattern=index)
                                value = re.findall(pattern=pattern, string=temp)
                                if value and int(max(value)) > int((xml[filterkey][key])):
                                    # warn = {
                                    #     key: max(value)
                                    # } }
                                    log = {key: max(value)}
                                    self.insertLogData(temp=temp, filterkey=xml[filterkey], log=log)
                                    # self.sendLogData(text=temp, warn=warn, xml=xml_dict, filterkey=filterkey, key=key)
                                    # warn = {}

                        elif str(filterkey) == "wupin":
                            pattern = re.compile(r'wupin:(\S+?);')
                            value = re.findall(pattern=pattern, string=temp)
                            if value:
                                match_split = value[0].strip().split("|")
                                match_dict = {x.split(",")[0]: x.split(",")[1] for x in match_split}
                            for key in xml["wupin"].keys():
                                if key in match_dict.keys():
                                    if int(match_dict[key]) > int(xml["wupin"][key]):
                                        # warn = {
                                        #     key: max(value)
                                        # }
                                        log = {key: match_dict[key]}
                                        self.insertLogData(temp=temp, filterkey=xml[filterkey], log=log)
                                        # self.sendLogData(text=temp, warn=warn, xml=xml_dict, filterkey=filterkey, key=key)
                                        # warn = {}=

            if flag:
                os.remove(path)
                # except Exception, e:
                #     print e

    def checkFileType(self, path):
        if path.endswith(".gz"):
            dst = self.decompassFile(path=path)
            decompass = True
            return dst, decompass
        else:
            decompass = False
            return path, decompass

    def decompassFile(self, path):
        gz = _GZipTool(bufSize=8192)
        decompress_path = path[:-3]
        return gz.decompress(gzFile=path, dst=decompress_path)

    def insertLogData(self, **kwargs):
        data = {
            "log": kwargs["log"],
            "filterkey": kwargs["filterkey"],
            "message": kwargs["temp"]
        }
        self.db["alarmLog"].insert_one(dict(data))

        # def sendLogData(self, text, warn, xml, filterkey, key):
        #     # try:
        #     url = 'https://oapi.dingtalk.com/robot/send?access_token=45f5f3675c7752a245324a79209c9d814aceb80e5d63a8d041e34fc3c4611baf'
        #     heards = {
        #         "Content-Type": "application/json ;charset=utf-8 "
        #     }
        #     string_textMsg = {
        #         "msgtype": "text",
        #         "text": {
        #             "content": {
        #                         "text": text,
        #                         "xml": xml[filterkey][key],
        #                         warn.keys()[0]: warn.values()[0]
        #             }
        #         }
        #     }
        #     string_textMsg = json.dumps(string_textMsg)
        #     requests.post(url, data=string_textMsg, headers=heards)
        # except Exception, e:
        #     print e


if __name__ == '__main__':
    # try:
    # config, _ = genParserClient()
    # folder_path = config.path
    # file_path = r'/home/zhouxiaoxi/Desktop/eventAlarm/game.log.2017-09-07-09'
    file_path = r'/home/zhouxiaoxi/Desktop/eventAlarm/new 2.txt'
    event = EventAlarm()
    xml_dict = event.parseXML()
    event.parseLogFile(file_path=file_path, xml=xml_dict)
    # except Exception, e:
    #     print e
