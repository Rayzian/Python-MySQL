#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
@author zhouxiaoxi
@简乐互动

"""

import csv
import os
import sys


def getCSVInfo(path, index_list):
    """
    获取CSV文件里的信息

    @param ;
            path: csv文件路径            type: str   例： r'C:\Users\zhouxiaoxi\Desktop\tarfloder\total.csv'
            index_list: 字段值列表       type: list  例： ["1003", "1004"]

    @return:
            file_dict: 字段值所对应的值  type: dict  例： {“1003”：[“200”, “5000”]}

    """
    file_dict = {}

    for index in index_list:
        file_dict[index] = []

    with open(path, "r") as cf:
        content = csv.reader(cf)
        for temp in content:
            if temp:
                index = temp[1]
                file_dict[index].append(temp[0])
    if file_dict:
        return file_dict
    else:
        return None


def getContent(target_file_list, file_dict):
    """
    获取激活码并写入新文件

    @param：
          target_file_list: 激活码文件路径      type: str   例：r"C:\Users\zhouxiaoxi\Desktop\tarfloder\1003.txt"
          file_dict: CSV文件字段值所对应的值    type: dict  例：{“1003”：[“200”, “5000”]}

    @return:
            None
    """
    count_list = []
    # 遍历激活码文件
    for target_file in target_file_list:
        file = target_file.strip().split("\\")
        folder_name = file[-1].strip()[0:4]
        base_path = sys.path[0]
        folder_path = os.path.join(base_path, folder_name)
        if os.path.exists(folder_path):
            path = folder_path + '\\'
            os.removedirs(path)
        os.makedirs(folder_path)

        # 打开有激活码的文件
        rf = open(target_file, "r")

        contents = rf.readlines()

        path_dict = {}
        # 遍历CSV文件
        switch = False
        start = 0
        end = 0
        for temp in file_dict[folder_name]:
            file_path = (os.path.join(folder_path, temp)) + ".txt"
            if os.path.exists(file_path):
                file_path = (os.path.join(folder_path, temp)) + "_{}".format(str(path_dict[temp])) + ".txt"
                path_dict[temp] += 1

            elif not os.path.exists(file_path):
                file_path = (os.path.join(folder_path, temp)) + ".txt"
                path_dict[temp] = 1

            # 打开新文件
            tf = open(file_path, "a+")
            # 遍历获取激活码
            if switch:
                start = end
                end = start + int(temp)
            else:
                end += int(temp)
            for content in contents[start:end]:
                tf.write(content)
                count_list.append(content)
                # contents.pop((contents.index(content)))

                if len(count_list) == int(temp):
                    switch = True
                    count_list = []
                    break
            tf.close()
        rf.close()


if __name__ == '__main__':
    path = r'C:\Users\zhouxiaoxi\Desktop\tarfloder\total.csv'
    target_file_list = [r"C:\Users\zhouxiaoxi\Desktop\tarfloder\1003.txt",
                        r"C:\Users\zhouxiaoxi\Desktop\tarfloder\1004.txt"]
    index = ["1003", "1004"]
    file_dict = getCSVInfo(path=path, index_list=index)
    getContent(target_file_list=target_file_list, file_dict=file_dict)
