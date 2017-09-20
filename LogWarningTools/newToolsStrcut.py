# -*- coding: utf-8 -*-

"""
@author zhouxiaoxi
@简乐互动

"""
import codecs
import traceback
import shutil
import time
from random import choice
import gzip
import requests
import json
import os
import re
import sys
from xml.dom.minidom import parse
import xml.dom.minidom
from argUtil import genParserClient
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH_FILE = "./daemon.log"
LOG_MODE = 'a'
LOG_MAX_SIZE = 2 * 1024 * 1024  # 2M
LOG_MAX_FILES = 4  # 4 Files: print.log.1, print.log.2, print.log.3, print.log.4
LOG_LEVEL = logging.DEBUG

LOG_FORMAT = "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d(%(funcName)s)] %(message)s"


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
        self.cumulative_dict = {}
        self.log_dict = {}

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
            print traceback.format_exc()

    def parseLogFile(self, file_path, json_dir):
        """
        解析日志文件
        :param file_path: 日志文件路径
        :param xml: xml文件
        :return: None
        """
        xml = self.parseXML()
        path, flag = self.checkFileType(path=file_path)
        file_name = path.replace("\\", "/").strip().split("/")[-1]
        json_name = os.path.join(json_dir, (file_name + ".json"))

        with open(path, "r") as f:
            switch = False
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
                                        if int(max(value)) > int((xml[filterkey][key])) or \
                                                        self.log_dict[pk]["cumulative"][key] > int(
                                                    (xml[filterkey][key])):
                                            self.log_dict[pk][key] = max(value)
                                            switch = True


                            elif str(filterkey) == "wupin":
                                if filterkey not in self.log_dict:
                                    self.log_dict[pk][filterkey] = {}
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
                                            self.log_dict[pk][filterkey][key] = int(match_dict[key])
                                            self.accumulate(pk=pk, key=key, xml=xml, filterkey=filterkey, temp=temp,
                                                            value=value,
                                                            match_dict=match_dict)
                                            if int(match_dict[key]) > int(xml["wupin"][key]["limit"]) or \
                                                            self.log_dict[pk]["cumulative"][key] > int(
                                                        (xml[filterkey][key]["limit"])):
                                                self.log_dict[pk][filterkey][key] = int(match_dict[key])
                                                switch = True
                except Exception, e:
                    print traceback.format_exc()

            try:
                if switch:
                    with codecs.open(filename=json_name, mode="a", encoding="utf-8") as f:
                        for log_key in self.log_dict.keys():
                            data = {
                                u"过滤规则": xml,
                                u"问题字段": self.log_dict[log_key],
                                u"pk": log_key,
                                u"日志文件名": file_name
                            }
                            print data
                            f.write((json.dumps(data, ensure_ascii=False)))
                            f.write("\n")

                    self.sendLogData(file=json_name)
            except Exception, e:
                print traceback.format_exc()

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
                        u"解析结果文件": kwargs["file"]
                    }
                }
            }
            textMsg = json.dumps(textMsg)

            print textMsg
            requests.post(url=choice(url_list), data=textMsg, headers=heards)
        except Exception, e:
            print traceback.format_exc()

    def accumulate(self, **kwargs):
        """
        对字段值进行累加
        :param kwargs: 参数键值对
        :return: None
        """
        try:
            if "cumulative" not in self.log_dict[kwargs["pk"]]:
                self.log_dict[kwargs["pk"]]["cumulative"] = {}
            if kwargs["key"] not in self.log_dict[kwargs["pk"]]["cumulative"]:
                self.log_dict[kwargs["pk"]]["cumulative"][kwargs["key"]] = 0
            if str(kwargs["filterkey"]) == "wupin":
                if str(kwargs["xml"][kwargs["filterkey"]][kwargs["key"]]["cumulative"]) == "true":
                    self.log_dict[kwargs["pk"]]["cumulative"][kwargs["key"]] += int(kwargs["match_dict"][kwargs["key"]])
            else:
                if str(kwargs["xml"][kwargs["filterkey"]]["cumulative"]) == "true":
                    self.log_dict[kwargs["pk"]]["cumulative"][kwargs["key"]] += int(max(kwargs["value"]))
        except Exception, e:
            print traceback.format_exc()

    def daemonize(self, dir_path, save_dir, json_dir):
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

        Logger.info("Daemon pid is: %s" % str(os.getpid()))
        with open('/dev/null') as read_null, open('/dev/null', 'w') as write_null:
            os.dup2(read_null.fileno(), sys.stdin.fileno())
            os.dup2(write_null.fileno(), sys.stdout.fileno())
            os.dup2(write_null.fileno(), sys.stderr.fileno())

        while True:
            try:
                for path, dir, files in os.walk(dir_path):
                    if files:
                        for file in files:
                            check_path = os.path.join(save_dir, file)
                            file_path = os.path.join(path, file)

                            # 检查保存日志文件的文件夹是否有该文件
                            if not os.path.exists(check_path):
                                Logger.info("Get log: %s" % str(file))
                                self.parseLogFile(file_path=file_path, json_dir=json_dir)
                                Logger.info("Parse log %s finished." % str(file))
                                Logger.info("Copy log to %s" % str(save_dir))
                                shutil.copy(file_path, save_dir)
                                Logger.info("Copy finished.\n")
                                os.remove(file_path)
                            else:
                                Logger.info("File is exists at %s\n" % str(check_path))
                                os.remove(file_path)
            except Exception, e:
                print traceback.format_exc()

                Logger.debug('Daemon exits at %s\n' % (time.strftime('%Y:%m:%d-%H:%m:%s', time.localtime(time.time()))))
                cmd = "sudo kill -9 %s" % str(os.getpid())
                os.system(cmd)
                return None


if __name__ == '__main__':
    try:
        # 获取日志文件目录
        config, _ = genParserClient()
        dir_path = config.path

        event = EventAlarm()

        # # 实例化logger对象
        handler = RotatingFileHandler(LOG_PATH_FILE, LOG_MODE, LOG_MAX_SIZE, LOG_MAX_FILES)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)

        Logger = logging.getLogger()
        Logger.setLevel(LOG_LEVEL)
        Logger.addHandler(handler)

        Logger.info('Daemon start up at %s' % (time.strftime('%Y:%m:%d-%H:%m:%s', time.localtime(time.time()))))

        # 创建保存日志文件的文件夹
        path = sys.path[0]
        save_dir = os.path.join(path, "gameLogTemp")
        json_dir = os.path.join(path, "result")
        if not os.path.exists(save_dir):
            os.mkdir(save_dir)
        if not os.path.exists(json_dir):
            os.mkdir(json_dir)
        event.daemonize(dir_path=dir_path, save_dir=save_dir, json_dir=json_dir)
    except Exception, e:
        print traceback.format_exc()
