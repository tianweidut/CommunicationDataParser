# -*- coding: UTF-8 -*-
'''
Created on 2012-4-29

@author: tianwei

Function: 给线程分配文件(天数，小时，RNC)(计算方法：输入文件序号范围，根据数据值计算是哪天，哪个小时，哪个RNC的文件)
'''
import sys,os,struct
import time,struct
from PyQt4.QtCore import *
from PyQt4.QtGui import *
import detailInfo

class Basic(QThread):
    def __init__(self,period,hour,rnc,parent=None):
        QThread.__init__(self,parent)
        self.period = period    #计算的总天数
        self.hour = hour        #每天应计算的小时数
        self.rnc = rnc          #RNC节点的数量
    
    #根据，当前序号转化成具体的天，小时，序号
    def convertID2Into(self,idnum):
        day  = idnum/(self.hour*self.rnc)          #周期中的第几天 （0-period-1）
        hour = (idnum - day*self.hour*self.rnc)/self.rnc  #在第多天的第多少小时 （0-hour-1）
        rnc  =  idnum - day*self.hour*self.rnc - hour*self.rnc 
        
        print day+1,hour+1,rnc
        
        return (day+1,hour,rnc)

class FileLoader(QMainWindow):
    def __init__(self,period,hour,rnc,start,end,threadNum,parent=None):
        QWidget.__init__(self,parent)
        self.period = period        #进程传递进来的基本信息
        self.hour = hour
        self.rnc = rnc
        self.start = start
        self.end = end
        self.threadNum = threadNum  #可以启动的线程的数量
        
        self.threadList = [Worker() for i in range(1,self.threadNum+2)]   #创建列表空间
        self.len = self.end - self.start +1
        self.num = self.len / self.threadNum + 1
        self.finishcnt = 0
        
    #线程分配
    def threadAlloc(self):
        single = detailInfo.SingleFile()
        for i in range(1,self.threadNum+1):
            self.threadList[i-1] =  Worker(self.period,self.hour,self.rnc)
            self.connect(self.threadList[i],SIGNAL("EmitFinished()"),self.emitfinished)
            start = (i-1)*self.num + self.start
            end = start + self.num -1
            if end > self.end:
                end = self.end
            print start,end,
            print':=============='
            for j in range(start,end+1):
                (day1,hour1,idnum1)=self.convertID2Into(j)
                print day1,hour1,idnum1
                single.singleFile(str(day1), str(hour1),str(idnum1))
            #self.threadList[i-1].render(start,end+1)
    
    def convertID2Into(self,idnum):
        day  = idnum/(self.hour*self.rnc)          #周期中的第几天 （0-period-1）
        hour = (idnum - day*self.hour*self.rnc)/self.rnc  #在第多天的第多少小时 （0-hour-1）
        rnc  =  idnum - day*self.hour*self.rnc - hour*self.rnc 
        
        print day+1,hour+1,rnc
        
        return (day+1,hour+1,rnc)
    
    def emitfinished(self):
        self.finishcnt += 1
        if self.finishcnt == self.threadNum:
            print 'finish all thread *_*'
            self.finishcnt = 0
            self.threadList = []
            
    def __del__(self):
        self.threadList = []
        
class Worker(Basic):
    def __init__(self,period=0,hour=0,rnc=0):
        Basic.__init__(self,period,hour,rnc)
        self.exiting = False
        self.startNum = 0
        self.end = 0
    #当work线程对象在被销毁的时候，需要停止线程
    def __del__(self):
        self.exiting = True
        self.wait()
    
    def finishEmit(self):
        self.emit(SIGNAL("EmitFinished()"))
    
    def render(self,startNum,end):
        self.startNum = startNum
        self.end = end
        
        if self.end < self.startNum:
            print 'start end Error'
        else:
            self.start()    #开始执行线程
        
    def run(self):
        #线程实际执行
        n = self.startNum
        single = detailInfo.SingleFile
        while not self.exiting and n<self.end+1:
            print self.startNum,self.end
            (day1,hour1,idnum1) = self.convertID2Into(n)
            print day1,hour1,idnum1
            #single.singleFile(int(day), int(hour), int(idnum))
            n += 1
        self.finishEmit()
        
def main():
    app = QApplication(sys.argv)
    starttime = time.clock()
    if len(sys.argv) < 7:
        print 'rewrite the argv!'
    else:
        period = int(sys.argv[1])
        hour = int(sys.argv[2])
        rnc = int(sys.argv[3])
        start = int(sys.argv[4])
        end = int(sys.argv[5])
        threadNum = int(sys.argv[6])
        print start,end
        f =  FileLoader(period,hour,rnc,start,end,threadNum)  #第一个小时，每天 7*24*70
        print '=====================start=============='
        f.threadAlloc()
        print '=====================end================'
        print (time.clock() - starttime)
        print 'End Time:' + time.ctime()   
    
                                                                                    
if __name__ == "__main__":
    main()