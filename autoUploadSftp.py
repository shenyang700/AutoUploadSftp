# -*- coding: utf-8 -*-
import os, sys
import paramiko
from stat import S_ISDIR
import time
import re

global remote_files
remote_files = []  # 存放服务器中已上传的db文件名
global timelog
timelog = []  # 日志


class configuration:
    HOST = []
    PORT = 0
    USER = []  # 用户名
    PASSWORD = []  # 用户密码
    INTERVAL = 2
    local_path = []  # 本地存贮路径
    sftp_path = []  # 需要下载的ftp目录路径
    FILEEXT = []
    TIME = 0  # 单位是小时，距离文件创建时间超过2小时才上传)

    def __init__(self):
        config_valid = {}
        lujing = self.__getmyPath()
        with open(lujing + "configuration.txt", "r", encoding='gbk') as f:  # 读取配置文件
            conf = f.readlines()
        for key, line in enumerate(conf):
            content = line.split('#', 1)
            content[0] = re.sub('\n$', "", content[0])  # 去掉文本里的 #
            content[0] = re.sub('\s+', '', content[0]).strip()  # 去掉所有空格
            if len(content[0]) > 0:
                temp = content[0].split('=', 1)
                config_valid[temp[0]] = temp[1]
        self.HOST = config_valid["HOST"]
        self.PORT = int(config_valid["PORT"])
        self.USER = config_valid["USER"]  # 用户名
        self.PASSWORD = config_valid["PASSWORD"]  # 用户密码
        self.INTERVAL = int(config_valid["INTERVAL"])
        self.local_path = config_valid["LocalDir"]  # 本地存贮路径
        self.sftp_path = config_valid["FTPDir"]  # 需要下载的ftp目录路径
        self.FILEEXT = config_valid['FILEEXT']
        self.TIME = int(config_valid['TIME'])  # 单位是小时，距离文件创建时间超过2小时才上传)

    def __getmyPath(self):  # 读取配置文件所在路径
        sap = '/'
        if sys.argv[0].find(sap) == -1:
            sap = '\\'
        indx = sys.argv[0].rfind(sap)
        path = sys.argv[0][:indx] + sap
        return path


def save_sftp_file_path(sftp_path, sftp, FILEEXT):
    """保存sftp指定路径下所有db文件的列表"""
    all_files_path = list()
    all_files = list()

    if sftp_path[-1] == '/':  # 去掉路径字符串最后的字符'/'，如果有的话
        sftp_path = sftp_path[0:-1]

    files = sftp.listdir_attr(sftp_path)  # 获取当前指定目录下的所有目录及文件，包含属性值
    for i in files:
        filename = sftp_path + '/' + i.filename
        if S_ISDIR(i.st_mode):  # 如果是目录，则递归处理该目录，这里用到了stat库中的S_ISDIR方法
            try:
                all_files.extend(save_sftp_file_path(filename, sftp, FILEEXT))
            except:
                continue
        else:
            file_name, file_ext = os.path.splitext(i.filename)
            if file_ext == FILEEXT:
                all_files_path.append(filename)
                all_files.append(i.filename)
    return all_files


def filelist(local_path, FILEEXT):
    '''
    获取local_path下的所有文件及目录信息
    '''
    all_files = list()
    dirs = list()
    files = os.listdir(local_path)  # 获取当前指定目录下的所有目录及文件，包含属性值

    for i in files:
        if os.path.isfile(os.path.join(local_path, i)):
            file_name, file_ext = os.path.splitext(i)
            if file_ext == FILEEXT:
                all_files.append(i)
        else:
            dirs.append(i)
    return all_files, dirs


def SftpUploadDir(sftp, sftp_path, local_path, TIME, FILEEXT):
    '''
    递归上传sftp指定目录下的所有文件及目录
    '''
    print(f'Walking to {local_path}')

    dirname = os.path.basename(local_path)  # 否则本地目录名与sftp目录名一样

    sftp.chdir(sftp_path)  # 进入sftp对应目录
    os.chdir(local_path)  # 进入本地上传目录

    try:
        sftp.mkdir(dirname, mode=755)  # 否则在本地创建该目录
    except OSError:
        sftp.chdir(dirname)
    else:
        sftp.chdir(dirname)  # 创建完后进入该目录

    if sftp_path[-1] == '/':  # 去掉路径字符串最后的字符'/'，如果有的话
        sftp_path = sftp_path[0:-1]
    sftp_curr_dir = sftp_path + '/' + dirname  # 获取FTP当前目录路径
    local_curr_dir = os.getcwd()  # 获取本地当前目录路径
    dictf, dirs = filelist(local_curr_dir, FILEEXT)  # 调用filelist函数，递归本地当前目录下的所有文件及目录

    global remote_files
    global timelog
    for k in dictf:  # 获取到的文件名
        localStat = os.stat(os.path.join(local_curr_dir, k))
        if k not in remote_files:  # 文件名与已上传的文件做对比
            if time.time() - localStat.st_mtime > 60 * 60 * TIME:  # 修改时间超过TIME小时才上传
                sftp.put(os.path.join(local_curr_dir, k), sftp_curr_dir + '/' + k)  # k不在remote_files中说明该文件未上传过
                remote_files.append(k)
                timelog.append(f'have uploaded {k} at {time.ctime(time.time())}')
                print(f'uploading {k}')
        else:  # 文件已上传的话开始从断点续传
            try:
                remoteStat = sftp.stat(sftp_curr_dir + '/' + k)
            except IOError:  # 此本地文件在服务器存在同名文件，然后在服务器的目标路径再上传一次
                sftp.put(os.path.join(local_curr_dir, k), sftp_curr_dir + '/' + k)
                print(f'the same file exists in other dir,restart uploading {k}')
            else:  # 断点续传
                if localStat.st_size != remoteStat.st_size:
                    f_local = open(os.path.join(local_curr_dir, k), "rb+")
                    f_local.seek(remoteStat.st_size)
                    f_remote = sftp.open(sftp_curr_dir + '/' + k, "ab+")
                    tmp_buffer = f_local.read(100000)
                    while tmp_buffer:
                        f_remote.write(tmp_buffer)
                        tmp_buffer = f_local.read(100000)
                    f_remote.close()
                    f_local.close()
                    timelog.append(f'have restart uploaded {k} at {time.ctime(time.time())}')
                    print(f'continue uploading {k}')

    # global flag
    # if flag == 0:  # 只执行一次，删去在local_path下按时间排序的最后一个文件夹
    #     dirs = sorted(dirs, key=lambda x: os.path.getmtime(os.path.join(local_path, x)))
    #     del dirs[-1]
    #     flag = 1
    for d in dirs:  # 对子目录进行处理
        SftpUploadDir(sftp, sftp_curr_dir, os.path.join(local_curr_dir, d), TIME, FILEEXT)  # 调用自身，递归下载子目录中的文件


def sftpMain(config):
    trans = paramiko.Transport((config.HOST, config.PORT))  # 连接 ftp
    trans.connect(username=config.USER, password=config.PASSWORD)  # 输入用户名和密码
    sftp = paramiko.SFTPClient.from_transport(trans)
    global remote_files
    remote_files = save_sftp_file_path(config.sftp_path, sftp, config.FILEEXT)  # 保存服务器已有的db文件
    SftpUploadDir(sftp, config.sftp_path, config.local_path, config.TIME, config.FILEEXT)
    trans.close()


if __name__ == '__main__':
    config = configuration()
    while 1:
        sftpMain(config)
        # flag = 0  # 将排除最后一个文件夹的标识还原
        print('*******************本次上传已结束*******************')
        print('*******************log***********************')
        for i in timelog:
            print(i)
        print('*******************end***********************')
        time.sleep(config.INTERVAL)
