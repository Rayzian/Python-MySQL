# -*- coding: utf-8 -*-

"""
@author zhouxiaoxi
@简乐互动

"""
from random import choice
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
    """
    解压.gz文件
    """

    def __init__(self, bufSize):
        self.bufSize = bufSize
        self.fin = None
        self.fout = None

    def decompress(self, gzFile, dst):
        """
        解压.gz文件
        :param gzFile: 压缩文件路径
        :param dst: 解压后文件路径
        :return: 解压后文件路径
        """
        print "start to decompress: ", gzFile
        self.fin = gzip.open(gzFile, 'rb')
        self.fout = open(dst, 'wb')
        self.__in2out()
        print "decompress {} final.".format(gzFile)
        return dst

    def __in2out(self):
        """
        解压文件
        :return: None
        """
        while True:
            buf = self.fin.read(self.bufSize)
            if len(buf) < 1:
                break
            self.fout.write(buf)
        self.fin.close()
        self.fout.close()


class EventAlarm(object):
    def __init__(self):
        self.textMsg_list = []
        self.client = pymongo.MongoClient("172.16.214.128", 27017)
        self.db = self.client["eventAlarm"]

    def parseXML(self):
        """
        解析xml文件
        :return: 标签属性键值对
        """
        xml_dict = {}
        try:
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
        except Exception, e:
            print e.message

    def parseLogFile(self, file_path, xml):
        """
        解析日志文件
        :param file_path: 日志文件路径
        :param xml: xml文件
        :return: None
        """
        path, flag = self.checkFileType(path=file_path)

        with open(path, "r") as f:
            for temp in f:
                try:
                    for filterkey in xml.keys():
                        if temp and str(filterkey) in temp:
                            if str(filterkey) != "wupin":
                                for key in xml[filterkey].keys():
                                    index1 = r'%s:(\S+?);' % str(key)
                                    index2 = r'%s:(\S+)' % str(key)
                                    pattern1 = re.compile(pattern=index1)
                                    pattern2 = re.compile(pattern=index2)
                                    value = re.findall(pattern1, temp) if re.findall(pattern1, temp) else \
                                        re.findall(pattern2, temp)
                                    if value and int(max(value)) > int((xml[filterkey][key])):
                                        log = {key: max(value)}
                                        self.insertLogData(temp=temp, filterkey=xml[filterkey], log=log)
                                        self.sendLogData(text=temp, filterkey=xml[filterkey], log=log)

                            elif str(filterkey) == "wupin":
                                value = re.findall(r'wupin:(\S+?);', temp) if re.findall(r'wupin:(\S+?);', temp) else \
                                    re.findall(r'wupin:(\S+)', temp)
                                if value:
                                    wupin_split = value[0].strip().split("|")
                                    if wupin_split > 1:
                                        match_dict = {x.split(",")[0]: x.split(",")[1] for x in wupin_split}
                                    else:
                                        match_dict = {value[0].split(",")[0]: value[0].split(",")[1]}
                                    for key in xml["wupin"].keys():
                                        if key in match_dict.keys():
                                            if int(match_dict[key]) > int(xml["wupin"][key]):
                                                log = {key: match_dict[key]}
                                                self.insertLogData(temp=temp, filterkey=xml[filterkey], log=log)
                                                self.sendLogData(text=temp, filterkey=xml[filterkey], log=log)
                except Exception, e:
                    print e.message

            if flag:
                os.remove(path)

    def checkFileType(self, path):
        """
        检查文件类型，是否为.gz格式的压缩文件
        :param path: 文件路径
        :return: 文件路径和是否为被解压过的标记
        """
        if path.endswith(".gz"):
            dst = self.decompassFile(path=path)
            decompass = True
            return dst, decompass
        else:
            decompass = False
            return path, decompass

    def decompassFile(self, path):
        """
        解压文件
        :param path: 文件路径
        :return: 返回解压后的文件路径
        """
        gz = _GZipTool(bufSize=8192)
        decompress_path = path[:-3]
        return gz.decompress(gzFile=path, dst=decompress_path)

    def insertLogData(self, **kwargs):
        """
        向mongodb插入数据
        :param kwargs: 参数键值对
        :return: None
        """
        data = {
            "log": kwargs["log"],
            "filterkey": kwargs["filterkey"],
            "message": kwargs["temp"]
        }
        self.db["alarmLog"].insert_one(dict(data))

    def sendLogData(self, **kwargs):
        """
        使用钉钉机器人发送告警日志信息
        :param kwargs: 参数键值对
        :return: None
        """
        try:
            url_list = [
                "https://oapi.dingtalk.com/robot/send?access_token=45f5f3675c7752a245324a79209c9d814aceb80e5d63a8d041e34fc3c4611baf",
                "https://oapi.dingtalk.com/robot/send?access_token=8be0858124a5e17fb9176f1bbd75490a238b8ef06162257a7b9768be6ca388c2",
                "https://oapi.dingtalk.com/robot/send?access_token=1abddbe37d09434dcee808a2ec8a68d5678c7b7680a943e12ede3ab0d10d8b72"]
            heards = {
                "Content-Type": "application/json ;charset=utf-8 "
            }
            textMsg = {
                "msgtype": "text",
                "text": {
                    "content": {
                        u"原日志信息": kwargs["text"],
                        u"过滤规则": kwargs["filterkey"],
                        u"问题字段": kwargs["log"]
                    }
                }
            }
            textMsg = json.dumps(textMsg)

            print textMsg
            requests.post(url=choice(url_list), data=textMsg, headers=heards)
        except Exception, e:
            print e.message


if __name__ == '__main__':
    try:
        config, _ = genParserClient()
        file_path = config.path
        event = EventAlarm()
        xml_dict = event.parseXML()
        event.parseLogFile(file_path=file_path, xml=xml_dict)
    except Exception, e:
        print e.message
