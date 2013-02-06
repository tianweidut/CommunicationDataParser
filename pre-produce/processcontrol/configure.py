# -*- coding: UTF-8 -*-
'''
Created on 2012-4-29

@author: tianwei

Function: 进程和线程的分配参数
'''
#Process and Thread
pthreadNums = 8     #每个进程的线程数量
pProcessNums = 20   #外部调用进程的数量 

# Time
pMonth = 31
pWeek = 7
pDay = 1
pHour= 24
pPeriod = pWeek   #处理周期，每周或每月  

#RNC Count
rncCount = 70   #基站数目

#Total nums
totalNum = rncCount * pHour * pPeriod  