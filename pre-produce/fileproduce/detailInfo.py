# -*- coding: UTF-8 -*-
'''
Created on 2012-4-29

@author: tianwei

Function: 生成基本信息（每一行内容+文件内容）
'''
import sys,os,string,struct
import configure
import time,random

class SingleFile(object):
    def __init__(self): 
        self.content = ""
        self.key = []
        self.filename = ''
        self.filepath = ''    #基本路径
        self.day = 0
        self.hour = 0
        self.id = 0
        self.createDate = [2012,5,1,0,0,0,0,0,0]
    #每个条目伪造
    def singelItem(self):
        #key 生成
        #随机生成秒数，以day和hour为界
        self.tick = float(time.mktime(self.createDate)) + float(int(self.day)-1)*86400 + float(int(self.hour)-1)*3600 + float(random.randint(0,3600))  #小时随机 
        self.strTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(self.tick))
        self.strTime = self.strTime +('%.3f'%(1.0/random.randint(5,100))).lstrip('0') 
        self.key.append('['+self.strTime+']')   #事件
        
        self.key.append(str(self.id))           #RNC Id编号
        
        for i in range(1,configure.keyCount):   #key1 - key%keyCount
            self.key.append('key'+str(i)+':'+str(random.randint(configure.startNum,configure.endNum)))
            self.key.append(random.choice('abc') + random.choice('cde') + random.choice('*%$'))
        #分隔符
        content = configure.separator.join(self.key)
        self.key = []   #快速释放
        return content+'\n'
    #单个文件生成    
    def singleFile(self,day,hour,id): #day:1-31 hour:0-23 id:1-70
        
        self.day = day
        self.hour = hour
        self.id = id
        
        self.filename = 'UEH_id-' +self.id +'_'+self.day + '-' + self.hour;
        self.path = configure.rootPath + '\\' + str(self.day) + '\\' + str(self.hour) + '\\';
        print self.path
        #创建文件夹
        if(os.path.isdir(self.path) == False):
            try:
                os.makedirs(self.path)
            except OSError,why:
                print "Failed:%s" % str(why)
        
        #创建文件
        file = open(self.path + self.filename,'w')
        for i in range(1,configure.signleFileCount):
            file.write(self.singelItem())
        file.close() 
    def __del__(self):
        self.key = [] 
           
def main():
    starttime = time.clock()
    test = SingleFile()
    print '=====================start=============='
    test.singleFile('1','1','1')
    print '=====================end=============='
    print (time.clock() - starttime);
    
if __name__ == "__main__":
    main()

