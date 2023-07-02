import requests

# 忽略警告信息
import warnings
warnings.filterwarnings('ignore')

# 导入第三方库来处理
# post请求方式为Content-Type:multipart/form-data所需要上传的数据以及文件
from MultipartFormdataEncoder import MultipartFormdataEncoder

def test_Api_all_null(url):
        """
		类型multipart/form-data：
        data：上传的是数据类型、
            accesskeyid：登录ID
            accesssecret：登录KEY  accesskeyid和accesssecret 在xms系统点击头像，点击accesskeys，点击生成获取
            machineid：机器ID XMS系统-设备管理-文件管理—中 可以看到设备 用于API的machine id
            tardir：上传服务器的目标路径
        file：上传的是文件类型 open本地文件
        """
        data=[('accesskeyid','c0046184f200f4db'),
              ('accesssecret','eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VySWQiOjEwMSwiU2FsdCI6ImMwMDQ2MTg0ZjIwMGY0ZGIiLCJpc3MiOiJ3ZW54aW5qaWVfMDIifQ'),
              ('machineid','170'),
              ('tardir','/home/admin/flash/')]
        file=[('userfile','flash2022111603.tar.gz',open("D:\\flash\\publish\\20221116\\flash2022111603.tar.gz",'rb'))]

        # api头部信息，确认content—type类型
        #********-----确认boundary，去第三方库MultipartFormdataEncoder中修改boundary具体内容------*********#
        headers={'Content-Type': 'multipart/form-data;boundary=----WebKitFormBoundaryCkJhFxYGzR3KiE8K' }

        # 调用第三方库类，将上传信息处理成需要的格式
        content_type, data = MultipartFormdataEncoder().encode(data,file)

        # 查看生成的传入数据格式,注释掉这两句，什么类型的上传文件都可以。打开供调试使用
        # data=data.decode('utf-8')
        # print(data)

        # post方法上传信息
        # url是api地址，data是上传的数据
        # verify=False表示处理网页不安全不连接的的问题
        # headers=headers表示标明头部信息
        r = requests.post(url,data=data,verify=False, headers=headers)

        # 返回文本格式的响应
        result =r.text
        # 返回json格式的响应

        return result

if __name__ == '__main__':

    # 目标url地址
    url='https://xms.zts.com.cn/api/v1/tasks/uploadfiles'
    # 查看响应是否成功
    print(test_Api_all_null(url))




