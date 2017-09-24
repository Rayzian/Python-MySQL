# -*- coding:utf-8 -*-

"""
author zhouxiaoxi
简乐互动
"""

import logging
from logging.handlers import RotatingFileHandler

LOG_PATH_FILE = "./daemon.log"
LOG_MODE = 'a'
LOG_MAX_SIZE = 2 * 1024 * 1024  # 2M
LOG_MAX_FILES = 4  # 4 Files: print.log.1, print.log.2, print.log.3, print.log.4
LOG_LEVEL = logging.DEBUG

LOG_FORMAT = "%(asctime)s %(levelname)s [%(filename)s:%(lineno)d(%(funcName)s)] %(message)s"

# 实例化logger对象
handler = RotatingFileHandler(LOG_PATH_FILE, LOG_MODE, LOG_MAX_SIZE, LOG_MAX_FILES)
formatter = logging.Formatter(LOG_FORMAT)
handler.setFormatter(formatter)

Logger = logging.getLogger()
Logger.setLevel(LOG_LEVEL)
Logger.addHandler(handler)


def loggerInfo(string):
    """
    打印信息
    @param string: 需要打印的信息
    @return: None
    """
    Logger.info(string)


def loggerError(string):
    """
    打印错误信息
    :param string: 需要打印的信息
    :return:
    """
    Logger.error(string)
