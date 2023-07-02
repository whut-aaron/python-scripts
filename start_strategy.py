import socket
import time
import datetime
import os
import sys
host_port_6666=("10.36.120.83",6666)
host_port_8888=("10.36.120.83",8888)

loss_file="../bin/api_flow/XtpApi_md/log/quote.log." + datetime.date.today().strftime("%Y%m%d")
#loss_file="../bin/api_flow/XtpApi_md/log/quote.log.20210707"
#str_match='discrete'
str_match='time out'

def login():
    sk = socket.socket()
    sk.connect(host_port_8888)
    return sk

def check_loss():
    lines = open(loss_file,'rU').readlines()
    miss_count = 0
    for line in lines:
        if line.find(str_match) > -1:
            miss_count+=1
    return miss_count

if __name__ == "__main__":
    abs_file = os.path.abspath(__file__)
    abs_path = os.path.split(os.path.realpath(__file__))[0]
    print(abs_path)
    os.chdir(abs_path)
    check_loss()
    sk = login()
    last_count = check_loss();
    while 1:
        count = check_loss()
        if(count > last_count):
            buff = struct.pack('iiii',16,142,count-last_count,count)
            sk.send(buff)

        last_count = count
        while 1:
            data = sk.recv(4096)
            print len(data)
            if len(data) < 4096:
                break
        time.sleep(3)

        today = datetime.datetime.now()
        if today.hour >= 16 :
            exit()

    sk.close()
