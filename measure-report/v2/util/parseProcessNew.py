#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-6-7         
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Modified: 2012-11-1
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

import os,sys,time,datetime
import uuid

######################
#importer
encoding = sys.getfilesystemencoding()
ROOT_DIR = os.path.abspath(
                os.path.join(os.path.dirname(unicode(__file__,encoding)),os.path.pardir))
#print ROOT_DIR
if os.path.exists(os.path.join(ROOT_DIR,'util')):
    sys.path.insert(0, ROOT_DIR)
    sys.path.insert(0, os.path.join(ROOT_DIR, 'bin'))
    sys.path.insert(0, os.path.join(ROOT_DIR, 'config'))    
    sys.path.insert(0, os.path.join(ROOT_DIR, 'util'))
else:
    raise Exception("Cannot find root dir.")

from config.strConfig import *
from util.keymanager import *
from util.loadInfo import loadInfo

class parseIndividual(object):
    """
    """
    def __init__(self, source_filename, all_lines, json_file_name, save_path, max_conn = 25):
        """
        """
        source_filename = unicode(source_filename, encoding)
        json_file_name = unicode(json_file_name, encoding)
        save_path = unicode(save_path, encoding)

        self.source_file_name = source_filename
        self.result_file_name = ""
        self.json_file_name = json_file_name
        self.all_lines =int(all_lines)
        
        self.save_path = save_path 
        
        self.item_dict = {}     #EventData中第一行原始字段列名和字段位置对应的字典数据结构。 
                                #Key为列名（检索关键词）,value 为位置
                                #列名 -> 获取位置 -> EventData原始数据中直接取值，填充到 1.Measurement_tabl
                                                                        #  2.call_fetch_correspond
                                                                        #  3.Eventdata_table
                                                                        #  4.Message_table        
        self.cell_dict = {}     #cellinformation 中 cell名字和cid的对应关系
        self.output_power_dict = {} #载入数据库的mspowerindividual 信息
        
        self.call_information = {}     #MS_Individual 对应   
        
        self.result_measurement_name = ""
        self.result_eventdata_name = ""
        self.result_callinformation_name = ""
        
        self.source_file = None
        self.result_file = None

        self.result_measurement_file = None
        self.result_eventdata_file = None
        self.result_callinformation_file = None                 
                    
        self.tmp_name = ""         
        self.handle_file_name()   
        
        self.content_cnt = 0
        
        self.result = self.init_result()
        self.saved_result = []      #保存已经写入ret的文件名称
            
        self.DBInfo = loadInfo(int(max_conn))
        
        self.init_dict()
    
    def init_dict(self):
        """
        Initial 各种字典
        """
        self.output_power_dict = self.DBInfo.loadOutputPower() #载入数据库的mspowerindividual 信息
        
        #call information 必备信息收集字典
        #扩展：    key -> MS Individual
        #          value: dictionary
        #                -> call meta data: list
        #                -> record_time: 计时点时间
        #                -> record_flag: 是否开启计时标记
        #                -> callid: 初始时指定uuid
        for cnt in range(0,65537):
            cnt = str(cnt)
            self.call_information[cnt] = {
                                          "write_line":[""]*len(callinoformation_table),     #call记录,最终写入的
                                          "record_time":None,
                                          "record_flag":False,
                                          "call_id":None,
                                          "CallStartTime":None,
                                          "CallEndTime":None,
                                          "Event_counter":None,     #event 数量累计
                                          "EndCallTime":None,
                                          "LastCellCallTime":None,
                                          "CellLastIndex":None,     #Cell 区别
                                          } 
 
    def init_result(self):
        ret = {}
        
        ret["MeasurementData"] = {}
        ret["EventData"] = {}
        ret["CallInformation"] = {}
        
#        for i in range(0,MAX_TIME_SPAN):
#            ret["MeasurementData"][i] = {
#                          "FileName":"",
#                          "SaveFileName":"",
#                          "DataStartTime":None,
#                          "Interval":3600,
#                          "TableID":None                      
#                    }
#            ret["EventData"][i] = {
#                          "FileName":"",
#                          "SaveFileName":"",
#                          "DataStartTime":None,
#                          "Interval":3600,
#                          "TableID":None                       
#                     }
#            ret["CallInformation"][i] = {
#                          "FileName":"",
#                          "SaveFileName":"",
#                          "DataStartTime":None,
#                          "Interval":3600,
#                          "TableID":None                       
#                     }
            
        return ret

    def init_split(self):
        """
        Split the files by hour
        """
        self.current_hour = {
                             "MeasurementData":-1,
                             "EventData":-1,
                             "CallInformation":-1,
                             }
        
        self.current_file = {
                             "MeasurementData":self.result_measurement_file,
                             "EventData":self.result_eventdata_file,
                             "CallInformation":self.result_callinformation_file,
                             }
        
        self.current_filename = {
                             "MeasurementData":self.result_measurement_name,
                             "EventData":self.result_eventdata_name,
                             "CallInformation":self.result_callinformation_name,
                             } 
         
        self.time_cnt = {
                         "MeasurementData":0,
                         "EventData":0,
                         "CallInformation":0,
                         }       
        
        self.first_entity = {
                         "MeasurementData":True,
                         "EventData":True,
                         "CallInformation":True,    
                             }  

    def readlinePartialSmall(self):
        """
        Async read file lines 
        """
        line = self.source_file.readline()
        while line:
            yield line
            line = (self.source_file.readline()).rstrip("\t").rstrip("\r").rstrip("\n")
            
        yield None
   
    def handle_file_name(self):
        """
        handle every file names
        """
        self.tmp_name = (os.path.basename(self.source_file_name)).split('.')[0]

        self.result_measurement_name = os.path.join(self.save_path, self.tmp_name+'_measurement_')
        self.result_eventdata_name = os.path.join(self.save_path, self.tmp_name+'_eventdata_')
        self.result_callinformation_name = os.path.join(self.save_path, self.tmp_name+'_callinformation_')
    
    def split_files(self,time_str,file_name):
        """
        split the files by time
        """
        get_hour = int(time_str.split(".")[0][8:10])

        if self.current_hour[file_name] == -1:
            self.current_hour[file_name] = get_hour
            self.current_file[file_name].close()
            #rename the init file name
            if os.path.exists(self.current_filename[file_name] +str(self.current_hour[file_name])):
                os.remove(self.current_filename[file_name]+str(self.current_hour[file_name]))
            os.rename(self.current_filename[file_name],self.current_filename[file_name]+str(self.current_hour[file_name]))
            
            self.current_file[file_name] = open(self.current_filename[file_name]+str(self.current_hour[file_name]),'w')
            #print "init:",self.current_hour[file_name]
            self.store_result(time_str,file_name)
                  
        if get_hour != self.current_hour[file_name]:
            #close the old file
            self.current_file[file_name].close()
            #update the hour
            self.current_hour[file_name] = get_hour
            #update the file descriptor
            if self.first_entity[file_name]:
                self.current_file[file_name] = open(self.current_filename[file_name] + str(self.current_hour[file_name]),"w")
                self.first_entity[file_name] = False
            else:
                self.current_file[file_name] = open(self.current_filename[file_name] + str(self.current_hour[file_name]),"a")
            self.store_result(time_str,file_name)
            
            #print "change:",self.current_hour[file_name] 

    def store_result(self,time_str,file_name):
        """
        Store the result
        """
        time_str = time_str.split(".")[0]
        start_time = time_str[0:4] + "-" + time_str[4:6] + "-" + time_str[6:8] + " "+ \
                     time_str[8:10] + ":00:00"
        current_hour = str(int(time_str.split(".")[0][8:10]))
        
        save_filename = self.current_filename[file_name]+current_hour
        
        if save_filename not in self.saved_result:
            self.result[file_name][self.time_cnt[file_name]] ={
                                          "FileName":self.source_file_name,
                                          "SaveFileName":save_filename,
                                          "DataStartTime":start_time,
                                          "Interval":3600,
                                          }
            self.saved_result.append(save_filename)
            self.time_cnt[file_name] =  self.time_cnt[file_name] + 1         
          
    def parse(self):
        """
        """
        #check the filename
        if not os.path.exists(self.source_file_name):
            print "Sorry, the %s doesn't exist!,please check"%self.source_file_name
            return        

        ###################
        #create result file
        
        self.source_file = open(self.source_file_name,'r')
        self.result_measurement_file = open(self.result_measurement_name,'w')
        self.result_eventdata_file = open(self.result_eventdata_name,'w')
        self.result_callinformation_file = open(self.result_callinformation_name,'w')           
        
        self.init_split()        
  
        ####################################################
        #call information statistics and Single record write
        reader = self.readlinePartialSmall()
        line = reader.next()
        
        start = self.record_start_time("call information store")
        
        all_lines = 0
        
        start_line = True
        while line:
            all_lines += 1
            line = line.split(information_split)         
          
            #start line process
            if start_line:
                start_line = False
                self.start_line_process(line)
            else:
                if not self.filter_empty(line):
                    ########################################################################
                    #core signle process: callID Got, call record, mr record, eventID record
                    #are all wroten in one branch
                    self.core_process(line) 
            
            #next line
            line = reader.next()
        
        #Dict Remaining write
        self.remaining_write()
        
        self.record_finish_time(start, "call information store")      
        
        ######
        #close 
        #print self.result
        self.source_file.close()
        self.result_measurement_file.close()
        self.result_eventdata_file.close()
        self.result_callinformation_file.close()
        
        self.DBInfo.close()
        
        return self.result        
    
    def remaining_write(self):
        """
        剩余清除
        """
        for key in range(0,65537):
            if self.call_information[ms_individual]["Event_counter"] != None:            
                self.final_write_call(key)
    
    def core_process(self,line):
        #Single process: call, mr, eventdata 
        #call operator, generator callID
        callid = self.call_dict_line(line)
        #MR report and EventID
        self.normal_line(line,callid) 

    def filter_empty(self,line):
        """
        Filter: MSIndividual is empty
        """
        if line[self.item_dict[MS_INDIVIDUAL]] == "" or line[self.item_dict[MS_INDIVIDUAL]] == None:
            return True
        
        return False

    def normal_line(self,line,callid):
        """
        single normal line process
        """
        if "36" == line[self.item_dict[EVENTID]] or "37" == line[self.item_dict[EVENTID]]:
            #MeasurementData 表填充
            self.mr_line(line,callid) 
        else:
            #EventData 表填充
            self.eventdata_line(line,callid)
                
    def eventdata_line(self,line,callid):
        """
        EventData数据库表的填充，同时需要对message临时表进行处理
        """ 
        #######
        #Step 0
        event_len = len(eventdata_table) + len(message_table) - 1
        pos_eventdata = [""]*(event_len)  #最终要写入文件的顺序
        
        ###########################################
        #Step 1:寻找eventdata_correspond对应，通过字典寻找
        for item in eventdata_correspond.keys():
            origin = eventdata_correspond[item]     #原始数据字段（原始数据首行解析出的）
            if origin == None:
                continue
            else:
                pos_eventdata[eventdata_table[item]] = line[self.item_dict[origin]]
        
        ###############     
        #Step 2: 手工对应
        #ID
        pos_eventdata[eventdata_table["ID"]] = str(uuid.uuid4())
        #CallID: 根据统计字典表进行检索与对应        
        if callid == "":
            print "Error \t", line[self.item_dict[SEQUENCE]],"\t",line[self.item_dict[MS_INDIVIDUAL]]
        else:
            pos_eventdata[eventdata_table["CallID"]] = callid

        #cellid 获取
        pos_eventdata[eventdata_table["CellID"]] = self.get_cellid(line[self.item_dict[MO]])
        
        #TargetCellID 获取   
        pos_eventdata[eventdata_table["TargetCellID"]] = self.get_cellid(line[self.item_dict[TARGET_MO]])
        
        #BSCDecision 获取
        pos_eventdata[eventdata_table["BSCDecision"]] = self.get_bsc(line[self.item_dict[MO]])
        
        #Message 字段填充
        start_pos = eventdata_table["message_table"]
        for pos in message_table.keys():
            if self.item_dict.has_key(message_table[pos]):
                pos_eventdata[start_pos + pos] =  line[self.item_dict[message_table[pos]]]
            else:
                pos_eventdata[start_pos + pos] = ""
        
        ################
        #Step3: 暂时不做考虑
        #pos_eventdata[eventdata_table["CallEventID"]] = ...
        
        ################
        #Step4: 时间分割
        self.split_files(line[self.item_dict[TIMESTAMP]],"EventData")
        
        pos_eventdata[eventdata_table["Timestamp"]] = self.time_show(time_str=line[self.item_dict[TIMESTAMP]])
        
        #############
        #step5: 最终写入
        write_line = writeStrSplit.join(pos_eventdata)
        self.current_file["EventData"].write(write_line + "\n")
        
    def mr_line(self,line,callid):
        """
        EventID = 36 or 37 时，MR数据库表
        """
        eventid = line[self.item_dict[EVENTID]]
        if eventid != "36" and eventid != "37":
            return
        
        ###############
        #Step 0
        mr_len = len(measurement_correspond)
        pos_mr = [""]*mr_len        #最终要写入文件的顺序
        
        ##########################################
        #Step 1:measurement_correspond对应，通过字典寻找
        for item in measurement_correspond.keys():
            origin = measurement_correspond[item]       #原始数据字段（原始数据首行解析出的）
            if origin == None:
                continue
            else:
                if item in ["RxLevelUL", 
                            "RxLevelDL", 
                            "NeighborRxLevel1", 
                            "NeighborRxLevel2", 
                            "NeighborRxLevel3", 
                            "NeighborRxLevel4", 
                            "NeighborRxLevel5", 
                            "NeighborRxLevel6",
                            "RxLevAntA",
                            "RxLevAntB"]:
                    # sub 110
                    try:
                        temp = str(int(line[self.item_dict[origin]]) - 110) 
                    except:
                        #temp = line[self.item_dict[origin]]
                        temp = '-110'   #默认值
                    pos_mr[measurement_table[item]] = temp
                else:
                    pos_mr[measurement_table[item]] = line[self.item_dict[origin]]   
        
        ##############
        #Step 2: 手工对应
        #ID
        pos_mr[measurement_table["ID"]] = str(uuid.uuid4())
        #CallID: 根据统计字典表进行检索与对应
                
        if callid == "":
            #print "error MR", line[self.item_dict[SEQUENCE]]
            print "call ID error!"
            pass
        else:
            pos_mr[measurement_table["CallID"]]  = callid
                            
        #cellid 获取
        pos_mr[measurement_table["CellID"]] = self.get_cellid(line[self.item_dict[MO]])
        
        #neighbor cell
        pos_mr[measurement_table["NeighborCount"]] = "6"
        
        for cnt in range(1,7):
            name = 'NeighborID' + str(cnt)
            cellname = 'bss neighbour cell' + str(cnt)
            pos_mr[measurement_table[name]] = self.get_cellid(line[self.item_dict[cellname]])                            
        
        #PathLossUL 计算
        PathLossUL,PathLossDL = self.get_pathloss(line)
        #PathLossUL,PathLossDL = (0,0)
        pos_mr[measurement_table["PathLossUL"]] = PathLossUL
        pos_mr[measurement_table["PathLossDL"]] = PathLossDL
                
        #################
        #step 3： 暂时不做考虑
        #pos_mr[eventdata_table["MRDLossCounter"]] = ...
        #pos_mr[eventdata_table["OwnBCCH"]] = ...
        #pos_mr[eventdata_table["RxLevAntA"]] = ...
        #pos_mr[eventdata_table["RxLevAntB"]] = ...   
        #pos_mr[eventdata_table["TXID"]] = ...   
        
        #############
        #step4: 时间分割
        self.split_files(line[self.item_dict[TIMESTAMP]],"MeasurementData")      
        
        pos_mr[measurement_table["Timestamp"]] = self.time_show(time_str=line[self.item_dict[TIMESTAMP]])
        
        #############
        #step5: 最终写入
        write_line = writeStrSplit.join(pos_mr)
        self.current_file["MeasurementData"].write(write_line + "\n")        
        
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
    
    def time_show(self,time_str):
        """
        Time new format
        """
        integer_str = time_str.split(".")[0]
        float_str = time_str.split(".")[1]
        ret = integer_str[0:4] + "-" + integer_str[4:6] + "-" + integer_str[6:8] + " " + \
                integer_str[8:10] + ":" + integer_str[10:12] + ":" + integer_str[12:] + "." + float_str
        
        return ret  
            
    def get_bsc(self,field):
        """
        MO中获取BSD字段
        """
        if field == "" or field == None:
            return ""
        else:
            bsc_info = (field.split(",")[1]).split("=")[1] 
            return bsc_info
    
    def get_pathloss(self,line):
        """
        pathloss: 根据算法和数据库数据进行计算
        """
        AllocBand = line[self.item_dict[ALLOC]]
        MSPower   = line[self.item_dict[MS_POWER]]
        
        if AllocBand == "" or MSPower == "":
            return ("","")
        
        OutputPower = self.output_power_dict[AllocBand][MSPower]
        
        RxLevelUL = line[self.item_dict[measurement_correspond["RxLevelUL"]]]
        RxLevelDL = line[self.item_dict[measurement_correspond["RxLevelDL"]]]
        
        #计算公式
        PathLossUL = int(OutputPower) - int(RxLevelUL)  + 100
        PathLossDL = int(OutputPower) - int(RxLevelDL)  + 100
        
        return (str(PathLossUL),str(PathLossDL))
    
    def get_cellid(self,filed):
        """
        CellID: 根据filed值获取cellid (读取从数据库中的字典表)
        """
        
        if filed == "" or filed == None:
            return ""
        tmp = filed.split(",")
        
        params = (tmp[1].split("=")[1], tmp[2].split("=")[1]) #BSC and CELL
        ret = ','.join(params)
        #ret = params[1]
#        cellid = self.cell_dict.get(params,"")
#        if not cellid:
#            self.cell_dict = self.DBInfo.loadCellInfo(*params)
#            cellid=self.cell_dict[params]
#        ret=cellid[0:8]+"-"+cellid[8:12]+"-"+cellid[12:16]+"-"+cellid[16:20]+"-"+cellid[20:32]
        return ret
        
        #return "7788"
        
    def call_dict_line(self,line):
        """
        CallInformation 每行添加与统计
        """
        call_necessay_list = [""]*len(call_fetch_correspond)     #call information 必备信息列表
        ms_individual = line[self.item_dict[MS_INDIVIDUAL]]
        
        #meaningless filter
        if ms_individual == "":
            return
        
        #step1: 判断是否存在与call_information字典中: 目测每个msindividual只用了一次
        if self.call_information[ms_individual]["Event_counter"] == None: 
            is_new_call = True
        else:
            is_new_call = False
        
        #step2: call_fetch_correspond 直接对应,提取call的原始数据
        for item in call_fetch_correspond.keys():
            if item in ["rxlev up-link","rxlev down-link"]:
                try:
                    temp = str(int(line[self.item_dict[item]]) - 110)
                except:
                    #temp = line[self.item_dict[item]]
                    temp = '-110'  #默认值
                call_necessay_list[call_fetch_correspond[item]] = temp
            else:
                call_necessay_list[call_fetch_correspond[item]] = line[self.item_dict[item]] 
        
        #step3:生成callid，并进行call的统计运算
        callid = self.callinformation_statistics(is_new_call,call_necessay_list,ms_individual)
        
        return callid

    def callinformation_statistics(self,is_new_call,call_necessay_meta,ms_individual):
        """
        statistics: 根据传入的是否为新call和每个event提取的必要
        信息列表，进行call数据生成和callid返回
        """        
        #step1: 判断是否为新的call
        if is_new_call == True:
            #初始存储
            call_id = str(uuid.uuid4())          #初始时指定uuid
            self.call_information[ms_individual]["call_id"] = call_id
        
        #step2:累计运算
        #判断时候为结束行:若结束行，则清空该ms_individual记录；否则进行累加运算
        #每个历史: 以event id=3 和 时间间隔 进行分割 (判断新的call_meta数据)
        event_flag = call_necessay_meta[0]
        record_flag = self.call_information[ms_individual]["record_flag"]
        call_id = self.call_information[ms_individual]["call_id"]
        if event_flag != "3" and record_flag == False:
            #没有遇到结尾且不在计时范围内
            #基本信息刷新
            self.basic_call(call_necessay_meta,call_id,ms_individual)
        elif record_flag == True:
            #已经遇到结尾，需要判断是否在计时范围内
            record_time = self.call_information[ms_individual]["record_time"]
            if float(call_necessay_meta[call_fetch_correspond_Timestamp_pos])  <= float(record_time) + split_time:
                #在指定时间范围内
                #基本信息刷新
                self.basic_call(call_necessay_meta,call_id,ms_individual)
            else:
                #超过时间范围，需要重新进行统计,向文件中写入记录，同时清空各项标记
                #此条记录不用写入
                self.final_write_call(ms_individual)    #将该ms_individual对应记录写入
                
                #清空记录,写入新的记录
                call_id = self.clear_call_new_write(ms_individual,call_necessay_meta)
                
        else:
            #event_flag = 3 clear command 指令，可能意味着要结束，
            #此时需要更新record_time 和 record_flag
            record_flag = True  #开始进行标记
            record_time = call_necessay_meta[call_fetch_correspond_Timestamp_pos]
            call_id = self.call_information[ms_individual]["call_id"]
            #基本信息刷新
            self.basic_call(call_necessay_meta,call_id,ms_individual)
            self.call_information[ms_individual]["record_time"] = record_time
            self.call_information[ms_individual]["record_flag"] = record_flag
        
        return call_id    
    
    def clear_call_new_write(self,ms_individual,call_necessay_meta):
        """
        """        
        #完成一次分割，更新call id
        call_id = str(uuid.uuid4())
        
        #基本信息写入
        self.call_information[ms_individual]["write_line"] = []
        self.call_information[ms_individual]["write_line"] = [""]*len(callinoformation_table)
        
        self.call_information[ms_individual]["record_flag"] = False
        self.call_information[ms_individual]["record_time"] = None
        self.call_information[ms_individual]["call_id"]     = call_id
        self.call_information[ms_individual]["CallStartTime"]     = None
        self.call_information[ms_individual]["CallEndTime"]     = None
        self.call_information[ms_individual]["EndCallTime"]     = None
        self.call_information[ms_individual]["Event_counter"]     = None
        self.call_information[ms_individual]["LastCellCallTime"]     = None
        self.call_information[ms_individual]["CellLastIndex"]     = None
        
        self.basic_call(call_necessay_meta,call_id,ms_individual)
        
        return call_id
    
    def final_write_call(self,ms_individual):
        """
        Final Write: call 信息的一些统计
        """
        #时间计算
        CallStartTime = self.call_information[ms_individual]["CallStartTime"]
        CallStartTime_stand = CallStartTime.split(".")[0]
        CallStartTime_after = "0."+CallStartTime.split(".")[1]
        CallEndTime = self.call_information[ms_individual]["CallEndTime"]
        CallEndTime_stand   = CallEndTime.split(".")[0]
        CallEndTime_after   = "0."+CallEndTime.split(".")[1] 
        start_time = float(time.mktime(time.strptime(CallStartTime_stand,"%Y%m%d%H%M%S")))  + float(CallStartTime_after)
        end_time   = float(time.mktime(time.strptime(CallEndTime_stand,"%Y%m%d%H%M%S"))) + float(CallEndTime_after)
        CallDuration  = str(end_time - start_time)
        
        #目前没有处理该字段
        #CallEventID = ... 
        #TimeFromLastHOToCallEnd = ...
        #HOCount = ...
        #IntraCellHOCount = ...
        #IntraCellHOFailureCount = ...
        #HOReversionCount = ...
        
        #AVG运算
        lens = self.call_information[ms_individual]["Event_counter"]
        self.call_information[ms_individual]["write_line"][RxLevULAVG_pos] = str(float(self.call_information[ms_individual]["write_line"][RxLevULAVG_pos]) / float(lens) )
        self.call_information[ms_individual]["write_line"][RxLevDLAVG_pos] = str(float(self.call_information[ms_individual]["write_line"][RxLevDLAVG_pos]) / float(lens) )
        self.call_information[ms_individual]["write_line"][RxQualULAVG_pos] = str(float(self.call_information[ms_individual]["write_line"][RxQualULAVG_pos]) / float(lens) )
        self.call_information[ms_individual]["write_line"][RxQualDLAVG_pos] = str(float(self.call_information[ms_individual]["write_line"][RxQualDLAVG_pos]) / float(lens) )
        self.call_information[ms_individual]["write_line"][TAAVG_pos] = str(float(self.call_information[ms_individual]["write_line"][TAAVG_pos]) / float(lens) )
        
        #一次通话中最后一个小区的时间统计
        TimeInLastCell = str(float(self.call_information[ms_individual]["EndCallTime"]) -  
                             float(self.call_information[ms_individual]["LastCellCallTime"]))
        
        #写入write_line
        self.call_information[ms_individual]["write_line"][CallStartTime_pos] = self.time_show(CallStartTime)
        self.call_information[ms_individual]["write_line"][CallEndTime_pos] = self.time_show(CallEndTime)
        self.call_information[ms_individual]["write_line"][CallDuration_pos] = CallDuration
        self.call_information[ms_individual]["write_line"][TimeInLastCell_pos] = TimeInLastCell
        #时间分割
        self.split_files(CallStartTime,"CallInformation") 
        
        #写入文本
        tmp = writeStrSplit.join(self.call_information[ms_individual]["write_line"])
        self.current_file["CallInformation"].write(tmp + '\n')
                                   
    def digital(self,num):
        if num.isdigit():
            return int(num)
        else: return 0
        
    def basic_call(self,call_list,call_id,ms_individual):
        """
        每个event进行运算
        """
        #遍历写入的条目：可以直接覆盖的
        for item in callinoformation_table.keys():
            name  = callinformation_correspond[callinoformation_table[item]]
            if name != None:    #只替换不为空
                value = call_list[call_fetch_correspond[name]]
                self.call_information[ms_individual]["write_line"][item] = value if value != "" else self.call_information[ms_individual]["write_line"][item]           
        
        #guid写入:每个合法的call，第一次写入
        if self.call_information[ms_individual]["write_line"][0] == "" or self.call_information[ms_individual]["write_line"][0] == None:
            self.call_information[ms_individual]["write_line"][0] = str(uuid.uuid4())
        
        #CallStartTime: 第一次更新
        if self.call_information[ms_individual]["CallStartTime"] == None:
            self.call_information[ms_individual]["CallStartTime"] = call_list[call_fetch_correspond_Timestamp_pos]
        #CallEndTime：每次都进行更新
        self.call_information[ms_individual]["CallEndTime"] = call_list[call_fetch_correspond_Timestamp_pos]
        
        #cell id 计算
        OriginatingCellID = self.call_information[ms_individual]["write_line"][OriginatingCellID_pos]
        if OriginatingCellID == None or OriginatingCellID == "":        #只更新一次
            OriginatingCellID = self.get_cellid(call_list[CELL_POS])
        
        #以最后的为基准
        TerminatingCellID = self.get_cellid(call_list[CELL_POS])
        self.call_information[ms_individual]["write_line"][OriginatingCellID_pos] = OriginatingCellID
        self.call_information[ms_individual]["write_line"][TerminatingCellID_pos] = TerminatingCellID
        
        self.call_operator(ms_individual,call_list)
        
        #最后一个cell的通话时间
        self.call_information[ms_individual]["EndCallTime"] =  call_list[call_fetch_correspond_Timestamp_pos]
        
        #Last Cell ID update
        if self.call_information[ms_individual]["CellLastIndex"] != call_list[CELL_POS]:
            self.call_information[ms_individual]["CellLastIndex"] = call_list[CELL_POS]
            self.call_information[ms_individual]["LastCellCallTime"] = call_list[call_fetch_correspond_Timestamp_pos]            
                
        #call id 写入
        self.call_information[ms_individual]["write_line"][1] = call_id
        
    def call_operator(self,ms_individual,call_list):
        """
        统计运算
        """
        #MAX运算
        RxLevULMAX = self.digital(self.call_information[ms_individual]["write_line"][RxLevULMAX_pos])
        RxLevDLMAX = self.digital(self.call_information[ms_individual]["write_line"][RxLevDLMAX_pos])
        RxQualULMAX = self.digital(self.call_information[ms_individual]["write_line"][RxQualULMAX_pos])
        RxQualDLMAX = self.digital(self.call_information[ms_individual]["write_line"][RxQualDLMAX_pos])
        TAMAX = self.digital(self.call_information[ms_individual]["write_line"][TAMAX_pos])
        
        RxLevULMAX = self.digital(call_list[call_fetch_correspond_rxlev_ul_pos]) if self.digital(call_list[call_fetch_correspond_rxlev_ul_pos]) > RxLevULMAX else RxLevULMAX
        RxLevDLMAX = self.digital(call_list[call_fetch_correspond_rxlev_dl_pos]) if self.digital(call_list[call_fetch_correspond_rxlev_dl_pos]) > RxLevDLMAX else RxLevDLMAX
        RxQualULMAX = self.digital(call_list[call_fetch_correspond_rxqual_ul_pos]) if self.digital(call_list[call_fetch_correspond_rxqual_ul_pos]) > RxQualULMAX else RxQualULMAX
        RxQualDLMAX = self.digital(call_list[call_fetch_correspond_rxqual_dl_pos]) if self.digital(call_list[call_fetch_correspond_rxqual_dl_pos]) > RxQualDLMAX else RxQualDLMAX
        TAMAX = self.digital(call_list[call_fetch_correspond_timing_advance_pos]) if self.digital(call_list[call_fetch_correspond_timing_advance_pos]) > TAMAX else TAMAX
        
        self.call_information[ms_individual]["write_line"][RxLevULMAX_pos]= str(RxLevULMAX)
        self.call_information[ms_individual]["write_line"][RxLevDLMAX_pos] = str(RxLevDLMAX) 
        self.call_information[ms_individual]["write_line"][RxQualULMAX_pos] = str(RxQualULMAX) 
        self.call_information[ms_individual]["write_line"][RxQualDLMAX_pos]  = str(RxQualDLMAX)
        self.call_information[ms_individual]["write_line"][TAMAX_pos] = str(TAMAX)        
        
        #计数器
        if self.call_information[ms_individual]["Event_counter"] == None:
            self.call_information[ms_individual]["Event_counter"] = 1
        else:
            self.call_information[ms_individual]["Event_counter"] += 1
        
        #SUM - AVG 运算
        RxLevULSUM = self.digital(self.call_information[ms_individual]["write_line"][RxLevULAVG_pos])
        RxLevDLSUM = self.digital(self.call_information[ms_individual]["write_line"][RxLevDLAVG_pos])
        RxQualULSUM = self.digital(self.call_information[ms_individual]["write_line"][RxQualULAVG_pos])
        RxQualDLSUM = self.digital(self.call_information[ms_individual]["write_line"][RxQualDLAVG_pos])
        TASUM = self.digital(self.call_information[ms_individual]["write_line"][TAAVG_pos])
        
        RxLevULSUM = RxLevULSUM + self.digital(call_list[call_fetch_correspond_rxlev_ul_pos])
        RxLevDLSUM = RxLevDLSUM + self.digital(call_list[call_fetch_correspond_rxlev_dl_pos])
        RxQualULSUM = RxQualULSUM + self.digital(call_list[call_fetch_correspond_rxqual_ul_pos])  
        RxQualDLSUM = RxQualDLSUM + self.digital(call_list[call_fetch_correspond_rxqual_dl_pos]) 
        TASUM = TASUM + self.digital(call_list[call_fetch_correspond_timing_advance_pos]) 
        
        self.call_information[ms_individual]["write_line"][RxLevULAVG_pos]= str(RxLevULSUM)
        self.call_information[ms_individual]["write_line"][RxLevDLAVG_pos] = str(RxLevDLSUM) 
        self.call_information[ms_individual]["write_line"][RxQualULAVG_pos] = str(RxQualULSUM) 
        self.call_information[ms_individual]["write_line"][RxQualDLAVG_pos]  = str(RxQualDLSUM)
        self.call_information[ms_individual]["write_line"][TAAVG_pos] = str(TASUM)  
    
    def record_start_time(self,str):
        """
        """
        print str, time.ctime()
        start = time.time()    
        return start
    
    def record_finish_time(self,start,str_record):
        """
        """
        #print str_record, 
        print str_record,'End Time:', time.ctime(), 'Total Time:', str(time.time() - start)        
                            
    def write_json(self,ret):
        """
        """
        import simplejson
        target_file = open(self.json_file_name,"w")
        simplejson.dump(ret,target_file)
        target_file.close()                
        

if __name__ == "__main__":
    if len(sys.argv) == 6:  
        p = parseIndividual(source_filename = sys.argv[1],
                            all_lines = sys.argv[2],
                            json_file_name = sys.argv[3],
                            save_path = sys.argv[4],
                            max_conn = sys.argv[5])
    else:
        print "Error the count of the argv"
  
#    p = parseIndividual("D:\\MR\\source\\eventData_120419_085606.txt",600000,"D:\\MR\\test.json")
    ret =  p.parse()
    p.write_json(ret)
    
    
    
    
    
    
    
    
    
    
    
    
    