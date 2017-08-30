# -*- coding: utf-8 -*-

import re
import MySQLdb


class MakeUpInfo(object):
    def __init__(self, host, name, password, database):
        self.conn = MySQLdb.connect(host, name, password, database)
        self.cursor = self.conn.cursor()
        self.pattren = re.compile(r'[^()]+')

    def getTableInfo(self):
        sql = """select ua_str from ua_map"""

        try:
            self.cursor.execute(sql)
            info_tuple = self.cursor.fetchall()
            str_list = map(lambda x: x[0], info_tuple)
            return str_list
        except Exception, e:
            print "getTableInfo"
            print e

    def getPhoneInfo(self, target_str):
        target = []
        for temp in target_str:
            target = re.findall(pattern=self.pattren, string=temp)
        if target:
            return target


    def parsePhoneInfo(self, string, data):
        try:
            for str in string:
                if str == "AHC/1.0" or str == "null":
                    return
                if str.startswith("Linux") or str.startswith("iPad") or str.startswith("iPhone") or str.startswith(
                        "iPod") or str.startswith("TVM"):
                    split_str = str.strip().split(";")
                    if len(split_str) > 2:
                        if split_str[0].strip() == "Linux":
                            data["ua_os"] = "Android"
                            data["ua_version"] = (re.search(r'[0-9_.]+', str, re.IGNORECASE)).group()
                            data["ua_jixincode"] = (split_str[-1].split("Build"))[0].strip()

                        elif split_str[0].strip() == "TVM xx":
                            data["ua_os"] = "Android"
                            data["ua_version"] = (re.search(r'[0-9_.]+', split_str[4], re.IGNORECASE)).group()
                            data["ua_jixincode"] = (split_str[-1].split("Build"))[0].strip()

                        elif split_str[0].strip() == "iPad" or split_str[0].strip() == "iPhone" or split_str[
                            0].strip() == "iPod":
                            data["ua_jixincode"] = split_str[0].strip()
                            data["ua_os"] = "OS"
                            data["ua_version"] = (re.search(r'[0-9_.]+', split_str[1], re.IGNORECASE)).group()

                    elif len(split_str) == 2 and (split_str[0].strip() == "iPad" or
                                                          split_str[0].strip() == "iPhone" or
                                                          split_str[0].strip() == "iPod"):

                        data["ua_jixincode"] = split_str[0].strip()
                        data["ua_os"] = "OS"
                        data["ua_version"] = (re.search(r'[0-9_.]+', split_str[1], re.IGNORECASE)).group()

            print data
            self.inserData(data=data)
        except Exception, e:
            print "parsePhoneInfo"
            print e

    def inserData(self, data):
        sql = """update ua_map
                  set
                  ua_jixincode='%s', ua_os='%s', ua_version='%s'
                  where ua_str='%s'""" % (data["ua_jixincode"], data["ua_os"], data["ua_version"], data["ua_str"])

        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception, e:
            print "inserData"
            print e
            self.conn.rollback()


if __name__ == '__main__':
    make = MakeUpInfo(host='localHost', name="root", password="zhouxiaoxi", database="PhoneType")
    try:
        strList = make.getTableInfo()
        for temp_str in strList:
            target_str = temp_str.splitlines()
            for i in target_str:
                if not i:
                    continue
                i = i.strip().splitlines()
                string = make.getPhoneInfo(target_str=i)
                if string:
                    data = {}
                    data["ua_str"] = temp_str
                    make.parsePhoneInfo(string=string, data=data)
    except Exception, e:
        print "task failed."
        print e

