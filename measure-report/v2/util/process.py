#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-6-7         
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

import os,sys
import time,uuid,datetime


from config.strConfig import *
from util.keymanager import *
from util.loadInfo import DBInfo
from util.seperateFiles import seperateFiles
from threadParse import threadParseWorker

from timeHelper import record_start_time,record_finish_time


class processStr(object):
    """
    """
    def __init__(self,filename ,save_path ,parent = None):
        """c
        """        
        self.source_file_name = filename       
        self.save_path = save_path 
        
        self.json_file = [] 
        
        self.item_dict = {}     #EventData中第一行原始字段列名和字段位置对应的字典数据结构。 
                                #Key为列名（检索关键词）,value 为位置
                                #列名 -> 获取位置 -> EventData原始数据中直接取值，填充到 1.Measurement_tabl
                                                                        #  2.call_fetch_correspond
                                                                        #  3.Eventdata_table
                                                                        #  4.Message_table
        
        self.thread_num = 0
        self.thread_cnt = 0
        
        self.startline = None
        
    
    
    def parseProcess(self):
        """
        Parse Whole
        """
        ################
        #Check File Size
        size = os.path.getsize(self.source_file_name)
        if size > FILESIZE:
            print "Large, size:%d"%size
            return self.parseProcessLarge(size)
        else:
            print "Small, size:%s"%size
            return self.parseProcessSmall() 
        
          
 
#    ################################################      
#    ## new version   
#    def parseProcessLarge(self,size):
#        """
#        """
#        ###################
#        #check the filename
#        if not os.path.exists(self.source_file_name):
#            print "Sorry, the %s doesn't exist!,please check"%self.source_file_name
#            return
#         
#        #############
#        #Files lines
#        self.source_lines = 0       #souce meaningful file lines
#               
#        with open(self.source_file_name, 'r') as file:
#            line = file.readline()
#       
#        line = line.rstrip('\t').rstrip('\r').rstrip('\n')
#        self.startline = line
#       
#        line = line.split(information_split)
#        self.start_line_process(line)        
#        #####################
#        #Calculate Split Size
#        self.large_split_pos = []
#        self.largeSpiece(size)
#        
#        for f in self.seperate_file:
#            f.close() 
#           
#        start = record_start_time("[Large Files Seperate]")
#       
#        ########################################################
#        #文件分割
#        self.source_lines = seperateFiles(self.source_file_name,
#                                          self.seperate_file_name,
#                                          information_split,
#                                          self.item_dict,
#                                          MS_INDIVIDUAL)
#       
#        record_finish_time(start,"[Large Files Seperate]")        
#        ######
#        #close
#        
#       
#        print '--------------Finish Seperate Files-----------------'
#       
#        ##########################################################
#        #call init, parse start
#        self.parseFilesThread()           
    
    def parseProcessLarge(self,size):
        """
        """
        ###################
        #check the filename
        if not os.path.exists(self.source_file_name):
            print "Sorry, the %s doesn't exist!,please check"%self.source_file_name
            return
          
        #############
        #Files lines
        self.source_lines = 0       #souce meaningful file lines
                
        ######################
        #Async Read file lines
        reader = self.readlinePartial()
        line = (reader.next()).rstrip('\t').rstrip('\r').rstrip('\n')
        self.startline = line
        
        line = line.split(information_split)
        self.start_line_process(line)        
        #####################
        #Calculate Split Size
        self.large_split_pos = []
        self.largeSpiece(size)
        
        start = record_start_time("[Large Files Seperate]")
        
#        ###########################################################
#        #Seperate large file into small files by MSIndividual Value
#        while line:
#            line = reader.next()
#            self.seperateFiles(line)   
            
        ###########################################################
        #new Version
        self.source_lines += 1
        self.seperateLargeFiles(reader)
        
        record_finish_time(start,"[Large Files Seperate]")        
        ######
        #close
        for f in self.seperate_file:
            f.close()  
        
        print '--------------Finish Seperate Files-----------------'
        
        ##########################################################
        #call init, parse start
        self.parseFilesThread()
                                
    def largeSpiece(self,size):
        """
        """
        ###################
        #Calculate the size
        cnt = size/(FILESIZE) + 1
        self.content_cnt = MSSIZE/cnt
        
        self.cnt = cnt
        
        for i in range(0,cnt):
            end = i*self.content_cnt
            if end >= MSSIZE-1:
                break
            
        self.large_split_pos.append(end)
      
        #######################
        #Create seperated files  
        self.seperate_file_name = [None]*(cnt+1) 
        self.seperate_file = [None]*(cnt+1)
        self.json_file = [None]*(cnt+1)
         
        self.tmp_name = (os.path.basename(self.source_file_name)).split('.')[0]
       
        for i in range(0,cnt+1):
            tmpname = os.path.join(self.save_path, self.tmp_name+'_mid_' + str(i))
            self.json_file[i] = (tmpname + ".json")
            self.seperate_file[i] = open(tmpname,'w')
            self.seperate_file[i].write(self.startline + "\n")
            self.seperate_file_name[i] = tmpname
               
    def seperateLargeFiles(self,lineGenerator):
        line = lineGenerator.next()
        while line:
            self.source_lines  += 1  
        
            line_split = line.split(information_split)
        
            msid = line_split[self.item_dict[MS_INDIVIDUAL]]

            try:
                a = int(msid)
            except Exception:
                #print "cannot:",msid
                line = lineGenerator.next()
                continue
        
            fileno = (int(msid))%(self.cnt)
            #################
            #File lines count     
            self.seperate_file[fileno].write(line + "\n")
            line = lineGenerator.next()
      
    def seperateFiles(self,line):
        """
        """
        #################
        #File lines count          
        self.source_lines  += 1  
        
        if line == None:
            return
        
        line_split = line.split(information_split)
        
        msid = line_split[self.item_dict[MS_INDIVIDUAL]]
        #Empty
        if msid == "" or msid == None or msid == " " or msid =="\t":
            return
        try:
            a = int(msid)
        except Exception:
            #print "cannot:",msid
            return
        
        fileno = (int(msid))%(self.cnt)
        #################
        #File lines count     
        self.seperate_file[fileno].write(line + "\n")
    
    def readlinePartial(self):
        """
        Async read file lines 
        """
        self.source_file = open(self.source_file_name,'r')
        line = self.source_file.readline()
        while line:
            yield line
            line = (self.source_file.readline()).rstrip("\t").rstrip("\r").rstrip("\n")
        
        self.source_file.close()
        yield None    
        
    def parseFilesThread(self):
        """
        Process every single file from the seperate files
        """        
        #########################
        #Every Single File Thread       
        thread = [None] * (self.cnt + 1)
        
        self.thread_cnt = 0 
        self.thread_num = self.cnt + 1
        
        self.starttime = record_start_time("[Threads Process]")
        
        ##############
        #start thread
        print "------------ parseProcessLarge ----------------"        
        for i in range(0,self.cnt+1):
            thread[i] = threadParseWorker(source_filename= self.seperate_file_name[i], 
                      all_lines = self.source_lines, 
                      json_file_name = self.json_file[i] , 
                      save_path = self.save_path,
                      max_conn = self.cnt + 2)
            thread[i].start()
       
        ############
        #wait thread
        for i in range(0,self.cnt+1):
            thread[i].join()
            
#        thread_count_start = 0
#        thread_count_end = 0
#        while thread_count_end < self.cnt + 1:
#            thread_count_end = (thread_count_end + 10) if (thread_count_end + 10) < (self.cnt + 1) else (self.cnt + 1)
#            for i in range(thread_count_start, thread_count_end):
#                thread[i] = threadParseWorker(source_filename= self.seperate_file_name[i], 
#                                              all_lines = self.source_lines, 
#                                              json_file_name = self.json_file[i], 
#                                              save_path = self.save_path,
#                                              max_conn = self.cnt + 2)
#                thread[i].start()
#            for i in range(thread_count_start, thread_count_end):
#                thread[i].join()
#            thread_count_start += 10
                
        print "--------------------------------------------------"
        print "---------------All threads finished!--------------"
        
            
    def parseProcessSmall(self):
        """
        """
        json_file = os.path.join(self.save_path,"smalljson.json")
        self.thread_cnt = 0 
        self.thread_num = 1
        print "------------ parseProcessSmall ----------------"
        thread = threadParseWorker(source_filename= self.source_file_name, 
                      all_lines = 0, 
                      json_file_name = json_file , 
                      save_path = self.save_path)
        thread.start()
        self.json_file.append(json_file)
        thread.join()
        print "----------------------------------------------"
        print "----------------Thread Finish-----------------"
        

    def start_line_process(self,line):
        """
        handle the first line, get the necessary key-index.
        we can ignore the differences of different eventdata
        """
        cnt = 0
        for word in line:
            if word == "" or word == None or word == " ":
                continue
            self.item_dict[word] = cnt
            cnt = cnt + 1 
        
    def finish_process(self):
        """
        """
        self.thread_cnt += 1
        print "++", self.thread_cnt + "" + "from" + self.thread_num
        if self.thread_cnt == self.thread_num:
            print "Finish All files process"
            record_finish_time(self.starttime,"[Threads Process]")
            self.merge_json()
            
    ################################################################
    ##new version        
    def merge_json(self):
        """
        """
        import simplejson
        
        def getDictValue(python_Object,key_file,index,key3):
            try:
                value = python_Object[key_file][str(index)][key3]
            except:
                if key3 == "FileName" or key3 == "SaveFileName":
                    value = ""
                else: value = None
            return value
        
        ret = {"MeasurementData":[],
               "EventData":[],
               "CallInformation":[]}
        for file in self.json_file:
            with open(file, "r+") as f:
                fileStr = f.read()
                python_object = simplejson.loads(fileStr)                 
                for key_file in ["MeasurementData", "EventData" , "CallInformation"]:
                    if python_object[key_file].keys():
                        long = sorted(map(int,python_object[key_file].keys()))[-1]
                        for i in range(long + 1):
                            tempDict = {}
                            for key in ["FileName", "SaveFileName", "DataStartTime"]:
                                tempDict[key] = getDictValue(python_object,key_file,i,key)
                            tempDict["Interval"] = 3600
                            ret[key_file].append(tempDict)
        return ret
                   
#    def merge_json(self):
#        """
#        """
#        import simplejson
#        
#        def getDictValue(python_Object,key_file,index,key3):
#            try:
#                value = python_Object[key_file][str(index)][key3]
#            except:
#                if key3 == "FileName" or key3 == "SaveFileName":
#                    value = ""
#                else: value = None
#            return value
#        
#        ret = {"measurement_file":{},
#               "eventdata_file":{},
#               "callinformation_file":{}}
#        for file in self.json_file:
#            with open(file, "r+") as f:
#                fileStr = f.read()
#                python_object = simplejson.loads(fileStr)                 
#                for i in range(0,24):
#                    for key_file in ["measurement_file", "eventdata_file" , "callinformation_file"]:
#                        if ret[key_file].get(i) != None:
#                            index = sorted(ret[key_file].keys())[-1] + 1
#                        else:
#                            index = i
#                        ret[key_file][index] = {}
#                        for key in ["FileName", "SaveFileName", "DataStartTime", "TableID"]:    
#                            ret[key_file][index][key] = getDictValue(python_object,
#                                                                     key_file,
#                                                                     i,
#                                                                     key)
#                        ret[key_file][index]["Interval"] = 3600
#        return ret
#        pass     


if __name__ == "__main__":
    pass
    