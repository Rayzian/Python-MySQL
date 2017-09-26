#! usr/bin/python
# -*- coding: utf-8 -*-

from optparse import OptionGroup, OptionParser

def genParserClient(version='0.0.1'):
    parser = OptionParser('Usage: %prog [options] arg1 arg2 ...', version='%prog ' + str(version))
    group = OptionGroup(parser, u'危险参数',
                        u'参入一下参数存在风险，请自行斟酌.')

    group.add_option('-p', '--path',
                     default=False,
                     help=u'日志文件路径')

    group.add_option('-l', '--host',
                     default=False,
                     help=u'服务器地址')

    parser.add_option_group(group)

    return parser.parse_args()


if __name__ == '__main__':
    result, a = genParserClient()
    print result
    print a
    print result.path
