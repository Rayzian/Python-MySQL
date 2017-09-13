# -*- coding: utf-8 -*-
import os
import sys
import time
import shutil


def daemonize(pid_file, dir_path, save_dir):
    """
    创建守护进程

    :param pid_file: 保存进程id的文件
    :param dir_path: 日志文件夹路径
    :param save_dir: 保存日志的文件夹路径

    :return: None
    """

    pid = os.fork()
    if pid:
        sys.exit(0)

    os.chdir('/')
    os.umask(0)
    os.setsid()

    _pid = os.fork()
    if _pid:
        sys.exit(0)

    sys.stdout.flush()
    sys.stderr.flush()

    with open('/dev/null') as read_null, open('/dev/null', 'w') as write_null:
        os.dup2(read_null.fileno(), sys.stdin.fileno())
        os.dup2(write_null.fileno(), sys.stdout.fileno())
        os.dup2(write_null.fileno(), sys.stderr.fileno())

    with open(pid_file, 'a+') as f:
        f.write('Daemon start up at %s\n' % (time.strftime('%Y:%m:%d-%H:%m:%s', time.localtime(time.time()))))
        f.write("Daemon pid is: %s\n" % str(os.getpid()))

        while True:
            try:
                for path, dir, files in os.walk(dir_path):
                    if files:
                        for file in files:
                            check_path = os.path.join(save_dir, file)
                            file_path = os.path.join(path, file)

                            if not os.path.exists(check_path):
                                f.write("Get log: %s\n" % str(file))
                                cmd = "python main.py -p %s" % file_path
                                os.system(cmd)

                                shutil.copy(file_path, save_dir)
                                os.remove(file_path)
                            os.remove(file_path)
            except Exception, e:
                print e
                f.write('Daemon exits at %s\n' % (time.strftime('%Y:%m:%d-%H:%m:%s', time.localtime(time.time()))))
                cmd = "sudo kill -9 %s" % str(os.getpid())
                os.system(cmd)


if __name__ == '__main__':
    path = sys.path[0]
    file = "daemon.log"
    pid_file = os.path.join(path, file)
    save_dir = os.path.join(path, "gameLogTemp")
    dir_path = r"/home/zhouxiaoxi/Desktop/Log111"
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    daemonize(pid_file=pid_file, dir_path=dir_path, save_dir=save_dir)
