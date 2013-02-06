# -*- coding: utf-8 -*-
'''
Created on 2012-5-3

@author: tianwei

@func: 多进程入口，平均分配目录
'''

import sys,os,time,struct,subprocess
from configure import pMainProcessCnt,srcPath

def main():
    print "Start Time:", time.ctime()
    startClock = time.clock()
    print "--------------------------------------------------"
    for i in range(1,2):#pMainProcessCnt+1):
        cmdStr = "python processFiles.py "
        cmdStr = cmdStr + srcPath + str(i) +"\\  "
        cmdStr = cmdStr + str(i)
        print "Entity:"+ cmdStr
        subprocess.Popen(cmdStr)
         
    print "--------------------------------------------------"
    print "end Time", time.ctime()
    print "use seconds",time.clock() - startClock

if __name__ == "__main__":
    main()