#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-5-23         
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
import time,uuid,re
from config.strConfig import *
from util.logger import *
from util.loadInfo import DBInfo


class processStr(object):
    """
    """
    def __init__(self,filename = None,load_dict =None,table_id=None,log_path =None,save_path = None ):
        """"""
        #table_id
        self.table_id = table_id
        self.log_path  = log_path
        self.save_path = save_path 
        
        #文件名预处理
        self.source_file_name = filename
        self.result_file_name = ""
        self.log_file_name = ""
        
        self.result_file = None
        self.source_file = None
        self.log_file = None
        
        self.tmp_name = None
        self.handle_file_name()
        
        self.load_dict = load_dict  #载入关键字匹配的字典==> "key":"position"
        self.load_len = len(load_dict)
        self.recent_segment = {"":([""]*self.load_len)}    #最近的key存储
        
        self.time_cnt = 0
        self.current_hour= -1
        self.current_time = ""
        self.result = self.init_result() 
        

        
        #cid 与 cellid对应表字典
        self.cid_dict = {"":""}
        
        #Log
        self.log = Logger(self.source_file_name,
                          self.result_file_name,
                          self.log_file_name)
        
    def init_result(self):
        ret = {}
        
        for i in range(0,MAX_TIME_SPAN):
            ret[i] = {
                      "FileName":"",
                      "SaveFileName":"",
                      "DataStartTime":None,
                      "Interval":3600,
                      "TableID":"" 
                      }
        
        return ret
      
    def handle_file_name(self):
        """
        handle every file names
        """
        self.tmp_name = (os.path.basename(self.source_file_name)).split('.')[0]
        result_name = self.tmp_name + '_result_'
        log_name    = self.tmp_name + '_log.csv'
        
        self.result_file_name = os.path.join(self.save_path , result_name)   
        self.log_file_name    = os.path.join(self.log_path , log_name)             
        
    def parseProcess(self):
        """"""
        #check the filename
        if not os.path.exists(self.source_file_name):
            print "Sorry, the %s doesn't exist!,please check"%self.source_file_name
            return
        
        #create the result file
        self.result_file = open(self.result_file_name,"w")
        self.source_file = open(self.source_file_name,"r")
        self.log.start()
        
        #for-line process
        for line in  self.source_file.readlines():
            self.log.accumulator(ALL_LINES)
            #check whether is UeH line, not binary
            if not line.startswith(legal_line_key):
                self.log.accumulator(FILTER_NUM)
                continue
            words = []
            #check whether single UehException Line
            if sigle_ueh_key in line:
                if segment_ueh_key in line:  #the segment of uehexception Part 2 line
                    self.log.accumulator(PART2_NUM)
                    self.segment_part2_ueh(line)
                else:       #the only ueh exception line
                    self.singel_ueh(line)
            elif segment_key in line:       #ueh segment of uehexception starting line
                self.log.accumulator(START_NUM)
                self.segment_start_ueh(line)
            elif segment_part_key in line:  #ueh segment of uehexception part 1 line
                self.log.accumulator(PART1_NUM)
                self.segment_part1_ueh(line)
            else:
                #non-meaningful lines
                self.log.accumulator(NOT_MATCH)
                self.log.write_item("[not match]"+line)
                continue 
            
        #close the file
        print self.result
        self.log.end()
        self.result_file.close()
        self.source_file.close()  
        
        return self.result
    
    def store_result(self):
        
        self.current_time = self.current_time + " "+str(self.current_hour)+":00:00"
        time_array = time.strptime(self.current_time, "%Y-%m-%d %H:%M:%S") 
        time_mk = int(time.mktime(time_array) + 3600)
        
        start_time = self.current_time
        #end_time   = time.strftime("%Y-%m-%d %H:%M:%S",time_mk)
        
        self.result[self.time_cnt] = {
                                      "FileName":self.source_file_name,
                                      "SaveFileName":self.result_file_name+str(self.current_hour),
                                      "DataStartTime":start_time,
                                      "Interval":3600,
                                      "TableID":self.table_id   
                                     }
        self.time_cnt = self.time_cnt + 1        
    
    def split_files(self,time_str):
        """
        split the files by time, we only operate this in Type3 and Part1 in type1 or type2
        """
        tmp = time_str.split(" ")[1]
        get_hour = int(tmp.split(":")[0])
        if self.current_hour == -1:
            self.current_hour = get_hour
            self.result_file.close()
            #rename the init file name
            if os.path.exists(self.result_file_name+str(self.current_hour)):
                os.remove(self.result_file_name+str(self.current_hour))
            os.rename(self.result_file_name,self.result_file_name+str(self.current_hour))
            
            self.result_file = open(self.result_file_name+str(self.current_hour),'w')
            print "init:",self.current_hour
            self.store_result()
        
        if get_hour > self.current_hour:
            
            #close the old file
            self.result_file.close()
            #update the hour
            self.current_hour = get_hour
            #update the file descriptor
            self.result_file = open(self.result_file_name + str(self.current_hour),"w")
            self.store_result()
            
            print "change:",self.current_hour
            
    def getKeyStr(self,keystr):
        keys = self.load_dict.keys()
        keystr_index = [key.lower() for key in keys].index(keystr.lower())
        return keys[keystr_index]
            
    def singel_ueh(self,line):
        """
        single ueh process: type 3
        """
        #final writing words
        newLineList = [""]*self.load_len
        word_all = line.split(information_split)
        if len(word_all) == 1 and information_split not in line:        #处理特殊情况，存在不以;作为分割的
            word_all = line.split(information_second_split)
        
        '''first word'''
        newLineList = self.basic_value_get(word_all[0], newLineList)
        #add time-file split

#        self.split_files(newLineList[self.load_dict[RECORD_TIME_STR]])
        self.split_files(newLineList[self.load_dict[self.getKeyStr(RECORD_TIME_STR)]])
            
        '''key-value matching'''
        #other: key-value
        newLineList = self.key_value_get(word_all[1:],newLineList,line)
        
        '''cellid <-> CID matching'''
        newLineList[self.load_dict[self.getKeyStr(CID_STR)]] = self.cellid_match(newLineList[self.load_dict[self.getKeyStr(CID_STR)]])
        
        '''write line'''
        newLineList[0] = str(uuid.uuid1())
        write_line = writeStrSplit.join(newLineList)
        self.result_file.write(write_line + "\n")
        self.log.accumulator(SIGNLE_UEH)
        
    def cellid_match(self,cid):
        """
        cellid <-> CID 数据库对应
        """
        cellid = ""
        #print cid
        cid = str(cid)
        
        if cid == "" or cid == None or cid=="UNDEF":
            cellid = ""
            ret=""
        else:
            if not cid in self.cid_dict.keys():
                from util.loadInfo import DBInfo
                self.cid_dict = DBInfo.load_cellid_dict(CID=cid)
            cellid = self.cid_dict[cid]
            cellid = self.dealWithCellid(cellid)
                #print "not:",cellid,len(self.cid_dict)
            ret=cellid[0:8]+"-"+cellid[8:12]+"-"+cellid[12:16]+"-"+cellid[16:20]+"-"+cellid[20:32]
        #return cellid
        return ret
    
    def dealWithCellid(self,cellid):
        cellid = cellid[6:8] + cellid[4:6] + cellid[2:4] + cellid[0:2] + cellid[10:12] + cellid[8:10] + cellid[14:16] + cellid[12:14] + cellid[16:]
        return cellid
    
    def segment_start_ueh(self,line):
        """
        segment ueh lines: type 1 or type 2
        """
        start = line.find(KEY_STR) + len(KEY_STR)
        end = line.find(keyEndSplit,start)
        key = line[start:end].strip()
        
        self.recent_segment[key] = [""]*self.load_len
        
    def segment_part1_ueh(self,line):
        """
        segment ueh lines: type 1 or type 2
        """
        word_all = line.split(information_split)
        
        #key
        start = word_all[0].find(KEY_STR) + len(KEY_STR)
        end = word_all[0].find(segment_part_key)
        key = word_all[0][start:end].strip(" ")  
        if key in self.recent_segment:
            #datatime,slot,exceptionCode
            self.recent_segment[key] = self.basic_value_get(word_all[0], self.recent_segment[key])
            #add time-file split
            self.split_files(self.recent_segment[key][self.load_dict[self.getKeyStr(RECORD_TIME_STR)]])
            
            #other key-value
            self.recent_segment[key] = self.key_value_get(word_all[1:],self.recent_segment[key],line)
        else:
            self.log.write_item("[error in part 2]"+line)
            self.log.accumulator(ERROR_NUM)
            return
        
    def basic_value_get(self,word,newLineList):             #提取datetime,slot,exceptionCode 
        """
        get datetime,slot,exceptioncode (type1 or type2 in segment_part2)
        """
        #time and data
        start = word.find("[")
        end = word.find("]")
        newLineList[self.load_dict[self.getKeyStr(RECORD_TIME_STR)]] = word[(start+1):(end)]
#        newLineList[self.load_dict[RECORD_TIME_STR]] = word[(start+1):(end)]
        datetime_str = word[(start+1):(end)].split(" ")
        #newLineList[self.load_dict[DATE_STR]] = datetime_str[0] #一张位置映射表，直接映射到数据库中字段位置
        #newLineList[self.load_dict[TIME_STR]] = datetime_str[1]
        self.current_time =  datetime_str[0]
        
        #slot
        start = end+1
        end = word.find("/")
        newLineList[self.load_dict[self.getKeyStr(SLOT_STR)]] = word[(start+1):(end)]
#        newLineList[self.load_dict[SLOT_STR]] = word[(start+1):(end)] #一张位置映射表，直接映射到数据库中字段位置 
        
        #exception code: 可能存在没有的情况，此处填充-0000000
#        start = word.find(EXCEPTION_CODE)
#        exception_code_strs = [k for k,v in self.load_dict if v == self.load_dict[self.getKeyStr(EXCEPTION_CODE)]] #查找所有形式的exceptioncode字符串
        exception_code_strs = []
        ex_code_index = self.load_dict[self.getKeyStr(EXCEPTION_CODE)]
        for k in self.load_dict:
            if self.load_dict[k] == ex_code_index:
                exception_code_strs.append(k)
        start = sorted([word.lower().find(ex_code.lower()) for ex_code in exception_code_strs])[-1]
        if start == -1: #未找到exceptioin code
            newLineList[self.load_dict[self.getKeyStr(EXCEPTION_CODE)]] = UNFOUND_EXCEPTION_CODE 
        else:
            start = start + len(EXCEPTION_CODE) 
            newLineList[self.load_dict[self.getKeyStr(EXCEPTION_CODE)]] = word[(start+1):].lstrip(keyStrSplit).lstrip()
    
        return newLineList
    
    def key_value_get(self,word_all,newLineList,line):       #不会覆盖旧的有意义的变量，但会对重复的key赋最新的值
        """
        get the key-value in function 
        """
        if ''.join(word_all).lower().find(EXCEPTION_CODE.lower()) == -1 and line.lower().find(EXCEPTION_CODE.lower()) != -1:
            exstart = line.lower().find(EXCEPTION_CODE.lower())
            exend = line.lower().find(information_split,exstart)
            word_all.append(line[exstart:exend])
        load_dict_lower = [k.lower() for k in self.load_dict]
        try:
            causeCodeIndex = self.load_dict[self.getKeyStr(CAUSE_CODE)]
        except: causeCodeIndex = -1
        for word in word_all:
            if keyStrSplit in word:
                tmp = word.split(keyStrSplit)
                key = tmp[0].strip(" ")
                value = tmp[1].strip("\n").rstrip('"').strip(" ").strip(".")
                if key.lower() in load_dict_lower:
                    if self.load_dict[self.getKeyStr(key)] == causeCodeIndex:
                        value = self.caseCodeValue_get(word_all,value,line)
                    newLineList[self.load_dict[self.getKeyStr(key)]] = value if value!="UNDEF" else ""
        return newLineList         
        
    def caseCodeValue_get(self,word_all,causeValue,line):
        if causeValue == "UNDEF" or causeValue == "": return ""
        try:
            ret = int(causeValue)
        except:
            exCodeIndex = self.load_dict[self.getKeyStr(EXCEPTION_CODE)]
            exCodeList = []
#            exCodeList = [k for k,v in self.load_dict if v == exCodeIndex]
            for k in self.load_dict:
                if self.load_dict[k] == exCodeIndex:
                    exCodeList.append(k)
            exStr = ""
            for wd in word_all:
                if keyStrSplit in wd and filter(lambda ex:re.search(ex.lower(),wd.lower()),exCodeList):
                    exStr = wd
                    break
            if not exStr:
                raise Exception("CaseCode get Error:Cannot find key ExceptionCode when causevalue is not int:%s"%line)
            try:
                tmp = exStr.split(keyStrSplit)
                exValue = int(tmp[1].strip(" ").strip("\n").rstrip('"'))
            except:
#                raise Exception("CaseCode get Error, line:%s" % word_all)
                exValue = -1
            ret = DBInfo.get_causecode_value(causeValue,exValue)
        return str(ret)
    
    def segment_part2_ueh(self,line):
        """
        segment ueh lines: type 1 or type 2
        """
        #key
        word_all = line.split(information_split)
        start = word_all[0].find(segment_ueh_key) + len(segment_ueh_key)
        end = word_all[0].find(PART2_STR)
        key = word_all[0][start:end].strip(" ")
        #print self.recent_segment[key] 
        if key not in self.recent_segment:
            self.log.write_item("[error in part 2]"+line)
            self.log.accumulator(ERROR_NUM)
            return
        
        #other key-value
        self.recent_segment[key] = self.key_value_get(word_all[1:],self.recent_segment[key],line)        
        
        '''cellid <-> CID matching'''
        self.recent_segment[key][self.load_dict[self.getKeyStr(CID_STR)]] = self.cellid_match(self.recent_segment[key][self.load_dict[self.getKeyStr(CID_STR)]])
        '''write line'''
        self.recent_segment[key][0] = str(uuid.uuid1())        
        write_line = writeStrSplit.join(self.recent_segment[key])
        self.result_file.write(write_line + "\n")
        self.log.accumulator(OTHER_UEH)
        
        #del key
        del self.recent_segment[key]

if __name__ == "__main__":
    
    print "start Time", time.ctime()
    start = time.clock()
    
    from util.loadInfo import loadInfo

    p = processStr("G:\UEH\source\ECR28_0427.log",DBInfo.loadkey_static(),'123qwe456rt')
    
    ret = p.parseProcess()
    
    #print ret
    
    print "End Time", time.ctime()
    print "total time(seconds)", str(time.clock() - start)
