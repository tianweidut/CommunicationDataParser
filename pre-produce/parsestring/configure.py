# -*- coding: utf-8 -*-
'''
Created on 2012-5-2

@author: tianwei
'''

#字符串处理的分割
strSplit = ";"
keySplit = ":"

#处理文件夹数量
filesCnt = 70    #每个70个小文件合并成一个文件（只能是一天的合并），具体数量需要根据内存大小进行调整

#文件夹位置
filePath = "D:\\resultFile\\"
srcPath = "D:\\filetest\\"

#目录主进程
pMainProcessCnt = 7     #对应天数