# -*- coding: utf-8 -*-

import os
import MySQLdb
import traceback
import re
from gzipTool import GZipTool
from xml.dom.minidom import parse
import xml.dom.minidom
from logger import loggerError, loggerInfo


class DemoOne(object):
    def __init__(self, host, name, password, database):
        self.conn = MySQLdb.connect(host, name, password, database)
        self.cursor = self.conn.cursor()
        self.partten1 = r'%s:(\S+?);'
        self.partten2 = r'%s:(\S+?)'

    def getfile(self, path, file_name):
        file_path = []
        cmd = "find %s %s" % (path, file_name)
        cur_path = os.getcwd()
        paths = os.popen(cmd).read()
        if paths:
            paths = paths.splitlines()
            file_path = [cur_path + temp[1:] for temp in paths]

        return file_path

    def parserXML(self):
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

    def checkFileType(self, file_path):
        de = GZipTool(bufSize=8192)
        if file_path.endswith(".gz"):
            dst = de.decompress(path=file_path)
            decompass = True
            return dst, decompass
        else:
            decompass = False
        return file_path, decompass

    def parserFile(self, xml, file_path):
        xml = self.parserXML()
        path, flag = self.checkFileType(path=file_path)

        cmd = "grep -i '%s' %s" % (xml['key'], file_path)
        strings = os.popen(cmd)
        if strings:
            strings = strings.splitlines()
            for string in strings:
                index1 = r'%s:(\S+?);' % xml['key']
                index2 = r'%s:(\S+)' % xml['key']
                pattern1 = re.compile(pattern=index1)
                pattern2 = re.compile(pattern=index2)
                value = re.findall(pattern1, string) if re.findall(pattern1, string) else \
                    re.findall(pattern2, string)

                # TODO wait for new tasks


                # def insertData(self, data):
                # table_name = data["logkey"]
                # index_list = struct_dict[table_name]
                #
                # index_value_list = []
                # for index in index_list:
                #     if index in data:
                #         if not index.isdigit():
                #             index_value_list.append("'%s'" % data[index])
                #         else:
                #             index_value_list.append(data[index])
                #     else:
                #         data[index] = "NULL"
                #         index_value_list.append(data[index])
                # insert_sql = """INSERT INTO %s (%s)
                #                       VALUES (%s)""" % (table_name, ",".join(index_list), ", ".join(index_value_list))
                # try:
                #     self.cursor.execute(insert_sql)
                #     self.conn.commit()
                # except Exception, e:
                #     print e
                #     self.conn.rollback()
