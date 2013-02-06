# -*- coding: UTF-8 -*-
'''
Created on 2012-4-29

@author: tianwei

Function: 
'''
import sys,os,subprocess,time
from configure import *

def main():
    print 'Start Time:' + time.ctime()
    for i in range(0,pHour):
        start = (i)*pWeek*rncCount
        end = start + pWeek*rncCount -1
        print "["+str(i)+']'+str(start+1)+'->'+str(end+1)
        cmdStr = 'python ../../fileProduce/src/fileloader.py  ' 
        cmdStr = cmdStr + str(pPeriod)+' '+ str(pHour) +' '+ str(rncCount) +' '
        cmdStr = cmdStr + str(start)+' '+str(end) +' '+ str(pthreadNums)
        print cmdStr
        subprocess.Popen(cmdStr)

if __name__ == "__main__":
    main()