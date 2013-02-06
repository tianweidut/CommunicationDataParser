# -*- coding: utf-8 -*-
'''
Created on 2012-5-3

@author: tianwei

@func: 单一进程处理多个文件
'''
import os,sys,string,struct
import time
from configure import filesCnt,filePath
import sngleParse

class filesProcess(object):
    def __init__(self,path,day):
        self.basePath = path        #天入口的初始路径
        self.filesPath = []         #文件夹的集合（/天/小时,最后追溯到小时的文件夹）
        self.day = day                #处理那一天
        self.oldcnt = 0             #旧生成的文件数量
        self.newcnt = 1             #新文件的数量
        self.curFileName = ""
        self.fileDescription = None
        self.sigle = sngleParse.ParseSignleFile()
        self.fileWritePath = ""
        
    def run(self):
        '''
        根据传入的文件天路径，处理每一个文件在新的文件夹下
        '''
        #扫描所有文件
        self.fileSplit()
        if self.filesPath.__len__() == 0:
            print "no files need to be processed!"
            return 
        
        #初始第一个文件（写入）
        self.fileWritePath= filePath + str(self.day) + "\\"
        self.curFileName =  self.fileWritePath + str(self.newcnt) + ".csv"
        #创建文件夹
        if(os.path.isdir(self.fileWritePath) == False):
            try:
                os.makedirs(self.fileWritePath)
            except OSError,why:
                print "Failed:%s" % str(why)
        #print self.curFileName
        self.fileDescription = open(self.curFileName,'w')
        
        #对每个文件进行处理与合并
        for fname in self.filesPath:
            if self.oldcnt >= filesCnt:
                self.newcnt = self.newcnt + 1
                self.fileDescription.close()
                self.curFileName =  self.fileWritePath + str(self.newcnt) + ".csv"
                #print self.curFileName
                self.fileDescription = open(self.curFileName,'w')
                #self.sqlProcess(self.curFileName)   #sql插入
                self.oldcnt = 0
            else:
                #print self.curFileName
                self.sigle.run(fname, self.fileDescription)
                self.oldcnt = self.oldcnt + 1 
        
        #关闭文件
        self.fileDescription.close()
        #self.sqlProcess(self.curFileName)   #sql插入
                
            
    def fileSplit(self):
        '''
        根据传入的文件天路径，生成所有文件的一个索引字典
        '''
        if os.path.exists(self.basePath) == False:
            print "directionary doesn't exist"
            self.filesPath = []
            return
        #递归扫描该目录下所有文件，记录绝对文件名在self.filesPath中
        self.walk_dir(self.basePath)
    
    def walk_dir(self,dir,topdown=True):
        for root,dirs,files in os.walk(dir, topdown):
            for name in files:
                #print (os.path.abspath(os.path.join(name)))
                self.filesPath.append(os.path.join(root,name)) 
    
    def sqlProcess(self,filename):
        '''
        SQL 处理bluk insert 
        '''
        print "Thread Process开辟新的进程"
        print (os.path.abspath(os.path.join(filename)))
         
               
def main():
    #fd = filesProcess("/home/tianwei/workspace/pytest/src",1)
    #fd.fileSplit()
    #print fd.filesPath
    
    if(sys.argv) < 3:
        print 'rewrite the argv!'
    else:
        path = str(sys.argv[1])
        day = int(sys.argv[2])
        print path,day
        f = filesProcess(path,day)
        startTime = time.clock()
        print "[Process"+str(day)+"]start:", time.ctime()
        f.run()
        print "[Process"+str(day)+"]end: ", time.ctime()
        print "[Process"+str(day)+"]use:",(time.clock()-startTime) 
        print "*_*"
        #f.fileSplit()
        #print f.filesPath    

if __name__ == "__main__":
    main()