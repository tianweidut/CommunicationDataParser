# -*- coding: UTF-8 -*-
'''
Created on 2012-7-18

@author: tianwei

Function: 

'''

import os, time
#from util import fileLock  
from multiprocessing import Process,Array,RLock

"""
多进程分块读取文件
"""

PROCESSORNUM = 6 # 6个进程时效率最快
BLOCKSIZE = 100000000 #100M
FILE_SOURCE = "D:\\MR\\source\\ATUTEST2210465292330515664201206271450_120627_145004.tmp"  #小文件1.14G
#FILE_SOURCE = "D:\\MR\\source\\ATUTEST5219307231012006613201206271720_120627_172001.tmp"  #大文件5.70G
SAVE_PATH = "D:\\MR\\result\\"

BIGNUM = 1000000000


class seperateFilesProcess(Process):
    def __init__(self, 
                 pid, 
                 rlock, 
                 array, 
                 file_source_name, 
                 save_files_name,
                 line_split,
                 item_dict,
                 individual_key):
        
        self.PID = pid
        self.rlock = rlock
        self.array = array
        self.source_file_name = file_source_name
        self.save_files_name = save_files_name
        self.save_path = os.path.dirname(self.save_files_name[0])
        
        self.filesNum = len(self.save_files_name)
        self.line_split = line_split
        self.item_dict = item_dict
        self.individual_key = individual_key
        
        self.file_size = os.path.getsize(self.source_file_name)
        super(seperateFilesProcess, self).__init__()
    
    def run(self):
        '''
                    进程处理
        Args:
            pid:进程编号
            array:进程间共享队列，用于标记各进程所读的文件块结束位置
            file:所读文件名称
                    各个进程先从array中获取当前最大的值为起始位置startpossition
                    结束的位置endpossition (startpossition+BLOCKSIZE) if (startpossition+BLOCKSIZE)<FILE_SIZE else FILE_SIZE
        if startpossition==FILE_SIZE则进程结束
        if startpossition==0则从0开始读取
        if startpossition!=0为防止行被block截断的情况，先读一行不处理，从下一行开始正式处理
        if 当前位置 <=endpossition 就readline
                    否则越过边界，就从新查找array中的最大值
        '''
        fstream = open(self.source_file_name,'r')
        save_files = []
        for i in range(len(self.save_files_name)):
            f = open(self.save_path + '\\' + str(i) + '_jobs_' + str(self.PID),'a+')
            save_files.append(f)
            
        source_lines = 0
        while True:
            
            self.rlock.acquire()
        
            startpossition = self.array[0] + self.array[1] * BIGNUM
            curpos = (startpossition + BLOCKSIZE) if (startpossition + BLOCKSIZE) < self.file_size else self.file_size 
            self.array[0] = curpos % BIGNUM
            self.array[1] = curpos // BIGNUM

            self.rlock.release()
            endpossition = curpos
        
            if startpossition == self.file_size:
                print "PID:%d END" % (self.PID)
                break
            if startpossition != 0:
                fstream.seek(startpossition)
            fstream.readline()
            pos = fstream.tell()

            while pos < endpossition:
                #do something for line
                source_lines += 1
                line = fstream.readline()
                line = line.rstrip("\t").rstrip("\r").rstrip("\n")
                
                line_split = line.split(self.line_split)
                
                id = line_split[self.item_dict[self.individual_key]]
                try:
                    int(id)
                except:
                    continue
                finally:
                    pos = fstream.tell()
                
                fileNo = (int(id))%(self.filesNum)
                #fileLock.lock(save_files[fileNo], fileLock.LOCK_EX | fileLock.LOCK_NB)
                save_files[fileNo].write(line + "\n")
                #fileLock.unlock(save_files[fileNo])
                
        self.rlock.acquire()
        source_lines += self.array[2] + self.array[3] * BIGNUM
        self.array[2] = source_lines % BIGNUM
        self.array[3] = source_lines //BIGNUM
        self.rlock.release()
        
        fstream.close()
        for f in save_files: f.close()  
        
def combineFiles(save_files_name):
    import subprocess
    print "Combine Files Start"
    processorNum = PROCESSORNUM
    save_path = os.path.dirname(save_files_name[0])
    for i in range(len(save_files_name)):
        cmd_str = 'copy /b ' + save_files_name[i] + '+' + '+'.join([save_path + '\\' + str(i) + '_jobs_' + str(PID) for PID in range(processorNum)]) + ' ' + save_files_name[i] + 'tmp'
        p = subprocess.Popen(cmd_str, shell=True)
        p.wait()
    print "Combine End"
    for file_name in save_files_name:
        os.remove(file_name)
        os.rename(file_name + 'tmp', file_name)
    
def seperateFiles(file_source_name, 
                  save_files_name, 
                  line_split, 
                  item_dict, 
                  individual_key):
    '''
    读取文件，写入给定文件，并返回读取行数
    file_source:数据文件名
    save_files:保存文件名数组
    line_split:行分割符
    item_dict:首行keys
    individual_key:保存文件 选择依据
    '''
    startTime = time.clock()
    print "File Size:", (os.path.getsize(file_source_name))
    print "Seperate Files Start:%s" % time.ctime()

    #进程数
    processorNum = PROCESSORNUM    
    
    rlock = RLock()
    array = Array('L', 4, lock = rlock)
    array[0] = array[1] = array[2] = array[3] = 0
    
    processList = []
    for i in range(processorNum):
        spF = seperateFilesProcess(i, 
                                   rlock, 
                                   array, 
                                   file_source_name, 
                                   save_files_name, 
                                   line_split,
                                   item_dict,
                                   individual_key)
        processList.append(spF)
        
    for i in range(processorNum):
        processList[i].start()
        
    for i in range(processorNum):
        processList[i].join()
        
    print "Finish Files Seperate:%s" % time.ctime()
    print "Totel Time(sec):", (time.clock() - startTime)
    combineFiles(save_files_name)
    
    return array[2] + array[3] * BIGNUM

if __name__ == '__main__':
    pass
