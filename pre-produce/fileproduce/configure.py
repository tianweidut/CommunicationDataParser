# -*- coding: UTF-8 -*-
'''
Created on 2012-4-29

@author: tianwei

Function: (24小时 * 70个节点 * 每个节点 30MB数据量)*30天 = 50G *30 =1.5TB (每周最多400G数据)
'''
# Time
pMonth = 31
pWeek = 7
pDay = 1
pHour= 24
pPeriod = pWeek   #处理周期，每周或每月

# File Size
signleFileCount = 3#30*1024#*1024      #30MB，每个文件，每行1KB

#RNC Count
rncCount = 70   #基站数目

#key count
keyCount = 80   #属性数目

#random
startNum = 1
endNum = 500

#分隔符
separator = ';'

#路径
rootPath = 'D:\\filetest2'
