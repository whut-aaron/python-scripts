# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 15:24:21 2020

@author: admin
"""
import paramiko
import os
import sys
import getpass
from datetime import datetime, timedelta
print("----------开始配置目标机器信----------")
#ips = input("主机IP:")
#user = input("主机账号:")
#password = getpass.getpass("主机密码:")
user = "zhangzzgo"
ips = "59.36.23.197"
password = "05f8f4c33ef25eda46fc"
port = 16888
class Tools(object):
    def __init__(self, user, password, port, ips):
        self.user = user
        self.password = password
        self.port = port
        self.ip = ips
    def connect(self):
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key = paramiko.RSAKey.from_private_key_file("D:\\flash\\zhangzzgo")
            self.ssh.connect(self.ip, self.port, self.user, pkey=key)
            print("----------连接已建立-----------------")
        except Exception as e:
            print("未能连接到主机")
    def cmd(self):
        cmd = input("请输入要执行的命令:>>")
        stdout, stdin, stderr = self.ssh.exec_command(cmd)
        #print(sys.stdout.read())
    def input(self):
#        self.local_file_abs = 
#        self.remote_file_abs = input("远程文件的绝对路径:>>")
        root_dir ="H:\\md\\price\\"
        remote_root_dir="/list/120.83_flash/market_data/"
        suffix_tar = trade_date.strftime("%Y%m%d") + ".tar.gz"
        suffix_md5 = trade_date.strftime("%Y%m%d") + ".md5"
        self.sse_price_local_file_abs = root_dir + "sse_price_" + suffix_tar
        self.sse_tbt_local_file_abs = root_dir + "sse_tbt_" + suffix_tar
        self.sze_price_local_file_abs = root_dir + "sze_price_" + suffix_tar
        self.sze_tbt_local_file_abs = root_dir + "sze_tbt_" + suffix_tar

        self.sze_price_md5_file =  root_dir + "sze_price_" + suffix_md5
        self.sze_tbt_md5_file =  root_dir + "sze_tbt_" + suffix_md5
        self.sse_price_md5_file =  root_dir + "sse_price_" + suffix_md5
        self.sse_tbt_md5_file =  root_dir + "sse_tbt_" + suffix_md5

        self.sse_price_remote_file_abs = remote_root_dir + "sse_price_" + suffix_tar
        self.sse_tbt_remote_file_abs = remote_root_dir + "sse_tbt_" + suffix_tar
        self.sze_price_remote_file_abs = remote_root_dir +"sze_price_" + suffix_tar
        self.sze_tbt_remote_file_abs = remote_root_dir +"sze_tbt_" + suffix_tar

    def printTotals(transferred, toBeTransferred):
        print("Transferred: {0}\tOut of: {1}".format(transferred, toBeTransferred))

    def put(self):
        sftp = paramiko.SFTPClient.from_transport(self.ssh.get_transport())
        sftp = self.ssh.open_sftp()
        self.input()
        sftp.put(self.local_file_abs,self.remote_file_abs)
    def get(self):
        now = datetime.now()
        print(now.weekday())
        if(now.weekday() >= 0 and now.weekday() < 5 and ((now.hour > 8 and now.hour < 16) or (now.hour == 8 and now.minute > 30))):
            print("交易时间，停止下载")
            exit()

        sftp = paramiko.SFTPClient.from_transport(self.ssh.get_transport())
        sftp = self.ssh.open_sftp()
        self.input()
        print("----------开始下载行情(" + trade_date.strftime("%Y%m%d") + ")------")


        if not os.path.exists(self.sze_price_md5_file) :
            print("----------开始下载SZE tick行情--------")
            sftp.get(self.sze_price_remote_file_abs,self.sze_price_local_file_abs)
            print("----------SZE tick 下载行情完成-------------")

            fp = open(self.sze_price_md5_file,'w')
            fp.close()

        if not os.path.exists(self.sze_tbt_md5_file) :
            print("----------开始下载SZE逐笔-------------")
            sftp.get(self.sze_tbt_remote_file_abs, self.sze_tbt_local_file_abs)
            print("----------SZE逐笔下载完成-------------")

            fp = open(self.sze_tbt_md5_file,'w')
            fp.close()

        if not os.path.exists(self.sse_price_md5_file):
            print("----------开始下载SSE tick 行情------")
            sftp.get(self.sse_price_remote_file_abs,self.sse_price_local_file_abs)
            print("----------SSE tick 下载行情完成------")

            fp = open(self.sse_price_md5_file,'w')
            fp.close()

        if not os.path.exists(self.sse_tbt_md5_file):
            print("----------开始下载SSE 逐笔-----------")
            sftp.get(self.sse_tbt_remote_file_abs, self.sse_tbt_local_file_abs)
            print("----------SSE下载逐笔完成------------")

            fp = open(self.sse_tbt_md5_file,'w')
            fp.close()
        print("----------下载行情(" + trade_date.strftime("%Y%m%d") + ")完成------")
        # self.close()

    def close(self):
        self.ssh.close()
        print("----------连接关闭--------------------")
    def set_trade_date(self, trade_date):
        self.trade_date = trade_date
        
obj = Tools(user, password, port, ips)
if __name__ == "__main__":
    msg = '''\033[32;1m
    执行命令 >>输入cmd
    上传文件 >>输入put
    下载文件 >>输入get
    退出     >>输入q\033[0m
    '''
    getattr(obj, "connect")()
#   while True:
#        print(msg)
#        inp = input("action:>>")
#        if hasattr(obj,inp):

    today = datetime.today()
    trade_date = today
    if(len(sys.argv) > 1):
        trade_date = datetime.strptime(sys.argv[1],"%Y%m%d")

    end_date = today
    if(len(sys.argv) > 2):
        end_date = datetime.strptime(sys.argv[2],"%Y%m%d")

    while trade_date <= end_date:
        obj.set_trade_date(trade_date);
        try:
         obj.get()
        except Exception as e:
            print("下载文件失败 : " + str(e))
        trade_date = trade_date + timedelta(days=1)
    
    print("----------download finished----------")
    obj.close()
    input("----------按任意键结束程序-------------")
#            getattr(obj,inp)()
#        if inp == "q":
#            getattr(obj,"close")()
#            exit()
#        else:print("没有该选项，请重新输入:>>")