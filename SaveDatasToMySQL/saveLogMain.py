#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import MySQLdb
import json
import gzip
import datetime
import threading
import time
from argUtil import genParserClient
from multiprocessing import Process, Queue

class _GZipTool(object):
    def __init__(self, bufSize):
        self.bufSize = bufSize
        self.fin = None
        self.fout = None

    def decompress(self, gzFile, dst):
        print "start to decompress :", gzFile
        print datetime.datetime.now()
        self.fin = gzip.open(gzFile, 'rb')
        self.fout = open(dst, 'wb')
        self.__in2out()
        print "decompress {} final.".format(gzFile)
        print datetime.datetime.now()

    def __in2out(self, ):
        while True:
            buf = self.fin.read(self.bufSize)
            if len(buf) < 1:
                break
            self.fout.write(buf)
        self.fin.close()
        self.fout.close()


class LogIntoDataBase(object):
    def __init__(self, host, name, password, database):
        self.conn = MySQLdb.connect(host, name, password, database)
        self.cursor = self.conn.cursor()

    def getfile(self, gzfile_path, q):
        for path, dir, file in os.walk(gzfile_path):
            gzfile_list = [path + "/" + i for i in file]
            map(lambda x: q.put(x), gzfile_list)

    def parserFile(self, q, table_list, struct_dict):
        gz = _GZipTool(bufSize=8192)
        decompress_path = ""
        try:
            path = q.get(True, timeout=5)
            print "-" * 45
            print "get GZFile %s succeed." % path
            print datetime.datetime.now()
            decompress_path = path[:-3]
            gz.decompress(gzFile=path, dst=decompress_path)
            with open(decompress_path, 'r') as text:
                print "getting log..."
                for temp in text:
                    json_object = json.loads(temp, encoding="utf-8")
                    if json_object["logkey"] in table_list:
                        self.insertData(json_object, struct_dict)
            if os.path.exists(decompress_path):
                os.remove(decompress_path)
            print "Get the {} log success".format(decompress_path)
            print datetime.datetime.now()
            print "-" * 45

        except (EOFError, Exception), e:
            print e
            print "Get the {} log failed.".format(decompress_path)
            print "-" * 45
            if os.path.exists(decompress_path):
                os.remove(decompress_path)

    def insertData(self, data, struct_dict):
        table_name = data["logkey"]
        index_list = struct_dict[table_name]

        index_value_list = []
        for index in index_list:
            if index in data:
                if not index.isdigit():
                    index_value_list.append("'%s'" % data[index])
                else:
                    index_value_list.append(data[index])
            else:
                data[index] = "NULL"
                index_value_list.append(data[index])
        insert_sql = """INSERT INTO %s (%s)
                      VALUES (%s)""" % (table_name, ",".join(index_list), ", ".join(index_value_list))
        try:
            self.cursor.execute(insert_sql)
            self.conn.commit()
        except Exception, e:
            print e
            self.conn.rollback()

    def getTableName(self):
        sql = """show tables"""
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        table_list = (map(lambda x: x[0], results))
        return table_list

    def getTableStruct(self, table_list):
        struct_dict = {}
        for table_name in table_list:
            sql = """select column_name from information_schema.COLUMNS where table_name='{}'""".format(table_name)
            self.cursor.execute(sql)
            table_index = self.cursor.fetchall()
            index_list = (map(lambda x: x[0], table_index))
            if "lmh" in index_list:
                index_num = index_list.index("lmh")
                index_list.pop(index_num)
            struct_dict[table_name] = index_list
        return struct_dict


if __name__ == '__main__':
    try:
        config, _ = genParserClient()
        folder_path = config.dir
        host = config.local
        name = config.user
        password = config.password
        database = config.database
        log_data = LogIntoDataBase(host=host, name=name, password=password, database=database)
        q = Queue()
        getgzfile_proc = Process(target=log_data.getfile, args=(folder_path, q,))
        getgzfile_proc.start()
        getgzfile_proc.join()
        thread_num = 0
        table_list = log_data.getTableName()
        struct_dict = log_data.getTableStruct(table_list=table_list)
        while (not q.empty()):
            log_data.parserFile(q=q, table_list=table_list, struct_dict=struct_dict)
        print "task final."
    except Exception, e:
        print e
        print "task failed."
