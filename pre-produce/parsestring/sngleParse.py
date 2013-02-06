# -*- coding: utf-8 -*-
'''
Created on 2012-5-2

@author: tianwei
'''

import sys,os,string,struct
import time
import uuid
from configure import strSplit,keySplit

class ParseSignleFile(object):
    def __init__(self,):
        self.targetFileDescription = None
        self.srcFileName = ""
        self.newLineList = []
        self.newLine = ""
    
    def __del__(self):
        self.targetFileDescription = None
        self.newLineList = []
        
    def run(self,srcFileName="",targetFileDescrption=None):
        #args
        self.targetFileDescription = targetFileDescrption
        self.srcFileName = srcFileName
        #check file
        if self.targetFileDescription == None or self.srcFileName == "":
            print "no files name!"
            return
        #open file 
        f = open(self.srcFileName)
        #read lines
        for line in f.readlines():
            #split in strings
            fields = line.split(strSplit)
            newLineList = []
            struuid = str(uuid.uuid1())
            newLineList.append(struuid)
            newLineList.append(fields[0].strip("[").strip("]"))
            for words in fields[1:]:
                if keySplit in words:  #可能存在字符是":"的，但不是key:value的错误
                    tmp = words.split(keySplit)
                    if tmp.__len__() == 2 and tmp[0] != None:   #标准key:value
                        newLineList.append(tmp[1])
                    else:                                       #以 keySplit 开头的字符串，如“:123”
                        newLineList.append(words)
                else:                                           #普通字符串
                    newLineList.append(words)
            #save the Line
            self.newLine = strSplit.join(newLineList)
            self.targetFileDescription.write(self.newLine)
            newLineList = []        
        #close file
        f.close
        
if __name__ == "__main__":
    t = open("test.csv","w")
    startTime = time.clock()
    print "start:", time.ctime()
    test = ParseSignleFile()
    test.run("UEH_id-0_1-1",t)
    print "end: ", time.ctime()
    print "use:",(time.clock()-startTime) 
    t.close()