# -*- coding: utf-8 -*-

"""
@author zhouxiaoxi
@简乐互动

"""
import codecs
import traceback
from random import choice
import gzip
import requests
import json
import os
import re
from xml.dom.minidom import parse
import xml.dom.minidom
from logger import loggerError, loggerInfo


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
        loggerInfo("start to decompress: %s" % gzFile)
        self.fin = gzip.open(gzFile, 'rb')
        self.fout = open(dst, 'wb')
        self.__in2out()
        loggerInfo("decompress %s final." % gzFile)
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
    def __init__(self, file_name, floder, json_dir, file_path, ip):
        self.cumulative_dict = {}
        self.log_dict = {}
        self.file_name = file_name
        self.floder = floder
        self.json_dir = json_dir
        self.file_path = file_path
        self.host = ip
        self.parseLogFile()

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
                    key_name = catalog.getAttribute("filterkey")
                    xml_dict[key_name] = {}

                for key in catalog.getElementsByTagName("filed"):
                    id = key.getAttribute("name")
                    value = key.getAttribute("limit")
                    cumulative = key.getAttribute("cumulative")
                    if str(key_name) == "wupin":
                        xml_dict[key_name][id] = {}
                        xml_dict[key_name][id]["cumulative"] = cumulative
                        xml_dict[key_name][id]["limit"] = value
                    else:
                        xml_dict[key_name][id] = value
                        xml_dict[key_name]["cumulative"] = cumulative

            if xml_dict:
                return xml_dict
        except Exception, e:
            loggerError(traceback.format_exc())

    def parseLogFile(self):
        """
        解析日志文件
        :param file_path: 日志文件路径
        :param xml: xml文件
        :return: None
        """
        json_path = os.path.join(self.json_dir, self.floder)
        if not os.path.exists(json_path):
            os.mkdir(json_path)

        xml = self.parseXML()
        path, flag = self.checkFileType(path=self.file_path)
        json_name = os.path.join(json_path, (self.file_name + ".json"))

        with open(path, "r") as f:
            switch = False
            value_dict = {}
            for temp in f:
                try:
                    for filterkey in xml.keys():
                        if temp and str(filterkey) in temp:
                            pk = re.findall(r'pk:(\S+?);', temp)[0]
                            if pk not in self.log_dict:
                                self.log_dict[pk] = {}
                            if str(filterkey) != "wupin":
                                for key in xml[filterkey].keys():
                                    if str(key) == "cumulative":
                                        continue

                                    index1 = r'%s:(\S+?);' % str(key)
                                    index2 = r'%s:(\S+)' % str(key)
                                    pattern1 = re.compile(pattern=index1)
                                    pattern2 = re.compile(pattern=index2)
                                    value = re.findall(pattern1, temp) if re.findall(pattern1, temp) else \
                                        re.findall(pattern2, temp)
                                    if value:
                                        self.accumulate(pk=pk, key=key, xml=xml, filterkey=filterkey, temp=temp,
                                                        value=value)
                                        if int(max(value)) >= int((xml[filterkey][key])) or (
                                                        key in self.log_dict[pk]["cumulative"] and
                                                        self.log_dict[pk]["cumulative"][key] >= int(
                                                        (xml[filterkey][key]))):
                                            switch = True
                                            if not max(value).startswith("-") and int(max(value)) >= int(
                                                    (xml[filterkey][key])):
                                                self.log_dict[pk][key] = max(value)


                            elif str(filterkey) == "wupin":
                                if filterkey not in self.log_dict[pk]:
                                    self.log_dict[pk][filterkey] = {}
                                value = re.findall(r';wupin:(\S+?);', temp) if re.findall(r';wupin:(\S+?);', temp) else \
                                    re.findall(r';wupin:(\S+)', temp)
                                if value:
                                    wupin_split = value[0].strip().split("|")
                                    if wupin_split > 1:
                                        match_dict = {x.split(",")[0]: x.split(",")[1] for x in wupin_split}
                                    else:
                                        match_dict = {value[0].split(",")[0]: value[0].split(",")[1]}
                                    for key in xml["wupin"].keys():
                                        if key in match_dict.keys():
                                            value_dict[key] = []
                                            self.accumulate(pk=pk, key=key, xml=xml, filterkey=filterkey, temp=temp,
                                                            value=value,
                                                            match_dict=match_dict)
                                            if int(match_dict[key]) >= int(xml["wupin"][key]["limit"]) or \
                                                            self.log_dict[pk]["cumulative"][key] >= int(
                                                        (xml[filterkey][key]["limit"])):
                                                value_dict[key].append(int(match_dict[key]))
                                                switch = True
                                                if not str(max(value_dict[key])).startswith("-") and int(
                                                        match_dict[key]) >= int(xml["wupin"][key]["limit"]):
                                                    if key not in self.log_dict[pk][filterkey]:
                                                        self.log_dict[pk][filterkey][key] = ""
                                                    self.log_dict[pk][filterkey][key] = max(value_dict[key])

                except Exception, e:
                    loggerError(traceback.format_exc())

            try:
                if switch:
                    with codecs.open(filename=json_name, mode="a", encoding="utf-8") as f:
                        for log_key in self.log_dict.keys():
                            data = {
                                u"过滤规则": xml,
                                u"问题字段": self.log_dict[log_key],
                                u"pk": log_key,
                                u"日志文件名": self.file_name,
                                u"服务器编号": self.floder
                            }
                            print data
                            f.write((json.dumps(data, ensure_ascii=False)))
                            f.write("\n")

                    self.sendLogData(file=json_name)
            except Exception, e:
                loggerError(traceback.format_exc())

            loggerInfo("Parse %s final." % self.file_name)

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
        if os.path.exists(decompress_path):
            os.remove(decompress_path)
        return gz.decompress(gzFile=path, dst=decompress_path)

    def sendLogData(self, **kwargs):
        """
        使用钉钉机器人发送告警日志信息
        :param kwargs: 参数键值对
        :return: None
        """
        try:
            current_folder = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/").strip().split("/")[-1]
            host = self.host
            ftp_path = "ftp://" + host
            file_name = kwargs["file"].replace("\\", "/").strip().split("/")[-1]
            path = ftp_path + "/" + current_folder + "/" + "result" + "/" + self.floder + "/" + file_name

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
                        u"解析结果文件路径": path
                    }
                }
            }
            textMsg = json.dumps(textMsg)

            requests.post(url=choice(url_list), data=textMsg, headers=heards)

            self.log_dict = {}
        except Exception, e:
            self.log_dict = {}
            loggerError(traceback.format_exc())

    def accumulate(self, **kwargs):
        """
        对字段值进行累加
        :param kwargs: 参数键值对
        :return: None
        """
        try:
            if "cumulative" not in self.log_dict[kwargs["pk"]]:
                self.log_dict[kwargs["pk"]]["cumulative"] = {}

            if str(kwargs["filterkey"]) == "wupin":
                if kwargs["key"] not in self.log_dict[kwargs["pk"]]["cumulative"]:
                    self.log_dict[kwargs["pk"]]["cumulative"][kwargs["key"]] = 0
                if str(kwargs["xml"][kwargs["filterkey"]][kwargs["key"]]["cumulative"]) == "true" and not \
                        kwargs["match_dict"][kwargs["key"]].startswith("-"):
                    self.log_dict[kwargs["pk"]]["cumulative"][kwargs["key"]] += int(kwargs["match_dict"][kwargs["key"]])
            else:
                if str(kwargs["xml"][kwargs["filterkey"]]["cumulative"]) == "true" and kwargs["key"] not in \
                        self.log_dict[kwargs["pk"]]["cumulative"]:
                    self.log_dict[kwargs["pk"]]["cumulative"][kwargs["key"]] = 0
                if str(kwargs["xml"][kwargs["filterkey"]]["cumulative"]) == "true" and not max(
                        kwargs["value"]).startswith("-"):
                    self.log_dict[kwargs["pk"]]["cumulative"][kwargs["key"]] += int(max(kwargs["value"]))
        except Exception, e:
            loggerError(traceback.format_exc())

    @classmethod
    def startParse(cls, file_path, json_dir, ip):

        file_name = file_path.replace("\\", "/").strip().split("/")[-1]
        floder = file_path.replace("\\", "/").strip().split("/")[-2]
        return cls(file_name=file_name, floder=floder, json_dir=json_dir, file_path=file_path, ip=ip)

#
# if __name__ == '__main__':
#     file_path = r'/home/zhouxiaoxi/Desktop/Log111/0001/20170922.gz'
#     ip = "127.0.0.1"
#     json_dir = r'/home/zhouxiaoxi/Desktop/LogWarningTools/result'
#     EventAlarm.startParse(file_path=file_path, json_dir=json_dir, ip=ip)