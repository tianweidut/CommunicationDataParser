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

class processStr(object):
    """
    """
    def __init__(self,filename = None,save_path = RESULT_DIR):
        """
        """
        self.source_file_name = filename
        self.result_file_name = ""
       
        self.save_path = save_path 
        
        self.item_dict = {}     #EventData中第一行原始字段列名和字段位置对应的字典数据结构。 
                                #Key为列名（检索关键词）,value 为位置
                                #列名 -> 获取位置 -> EventData原始数据中直接取值，填充到 1.Measurement_tabl
                                                                        #  2.call_fetch_correspond
                                                                        #  3.Eventdata_table
                                                                        #  4.Message_table
        
        self.cell_dict = {}     #cellinformation 中 cell名字和cid的对应关系
        self.output_power_dict = {} #载入数据库的mspowerindividual 信息
        
        self.call_information = {}
        
        self.callid_dict = []       #数组方式获取： 空间换时间
                                        
        self.source_buffer = None   #处理文件缓冲
        
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
        
        
        self.result = self.init_result()
        self.saved_result = []      #保存已经写入ret的文件名称
        
        self.init_dict()
    
    def init_dict(self):
        """
        Initial 各种字典
        """
        self.output_power_dict = DBInfo.loadOutputPower() #载入数据库的mspowerindividual 信息
        
        #call information 必备信息收集字典
        for cnt in range(0,65537):
            cnt = str(cnt)
            self.call_information[cnt] = []        
        
    def init_result(self):
        ret = {}
        
        ret["measurement_file"] = {}
        ret["eventdata_file"] = {}
        ret["callinformation_file"] = {}
        
        for i in range(0,MAX_TIME_SPAN):
            ret["measurement_file"][i] = {
                          "FileName":"",
                          "SaveFileName":"",
                          "DataStartTime":None,
                          "Interval":3600,
                          "TableID":None                      
                    }
            ret["eventdata_file"][i] = {
                          "FileName":"",
                          "SaveFileName":"",
                          "DataStartTime":None,
                          "Interval":3600,
                          "TableID":None                       
                     }
            ret["callinformation_file"][i] = {
                          "FileName":"",
                          "SaveFileName":"",
                          "DataStartTime":None,
                          "Interval":3600,
                          "TableID":None                       
                     }
            
        return ret
    
    def init_split(self):
        """
        Split the files by hour
        """
        self.current_hour = {
                             "measurement_file":-1,
                             "eventdata_file":-1,
                             "callinformation_file":-1,
                             }
        
        self.current_file = {
                             "measurement_file":self.result_measurement_file,
                             "eventdata_file":self.result_eventdata_file,
                             "callinformation_file":self.result_callinformation_file,
                             }
        
        self.current_filename = {
                             "measurement_file":self.result_measurement_name,
                             "eventdata_file":self.result_eventdata_name,
                             "callinformation_file":self.result_callinformation_name,
                             } 
         
        self.time_cnt = {
                         "measurement_file":0,
                         "eventdata_file":0,
                         "callinformation_file":0,
                         }       
        
        self.first_entity = {
                         "measurement_file":True,
                         "eventdata_file":True,
                         "callinformation_file":True,    
                             }  
    
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
            print "init:",self.current_hour[file_name]
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
        current_hour = time_str.split(".")[0][8:10]
        
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
                
    def parseProcess(self):
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
        ##################
        #store text buffer
        self.source_buffer = self.source_file.readlines()
        
        self.callid_dict = [""] * (len(self.source_buffer)+1) 
        
        ############################
        #call information statistics
        start_line = True
        for line in self.source_buffer:
            line = line.split(information_split)
            #start line process
            if start_line:
                start_line = False
                self.start_line_process(line)
            else:
                if not self.filter_empty(line):
                    self.call_dict_line(line)
        
        self.callinformation_statistics()   #call 统计，获得callinformation数据库表原始值，同时生成callid
        
        #print "End callinformation_statistics Time",time.ctime()
        #print "[callinformation_statistics]total time(seconds)", str(time.clock() - start)
                  
        #########################
        #eventdata and mr process
        start_line = True
        for line in self.source_buffer:
            line = line.split(information_split)
            #start line process
            if start_line:
                start_line = False
            else:   #normals lines
                if not self.filter_empty(line):
                    self.normal_line(line)  
        
        ######
        #close 
        #print self.result
        self.source_file.close()
        self.result_measurement_file.close()
        self.result_eventdata_file.close()
        self.result_callinformation_file.close()
        
        return self.result
    
    def filter_empty(self,line):
        """
        Filter: MSIndividual is empty
        """
        if line[self.item_dict[MS_INDIVIDUAL]] == "" or line[self.item_dict[MS_INDIVIDUAL]] == None:
            return True
        
        return False
    
    def normal_line(self,line):
        """
        single normal line process
        """
        if "36" == line[self.item_dict[EVENTID]]:
            #MeasurementData 表填充
            self.mr_line(line) 
        else:
            #EventData 表填充
            self.eventdata_line(line)
    
    def time_show(self,time_str):
        """
        Time new format
        """
        integer_str = time_str.split(".")[0]
        float_str = time_str.split(".")[1]
        ret = integer_str[0:3] + "-" + integer_str[4:5] + "-" + integer_str[6:7] + " " + \
                integer_str[8:9] + ":" + integer_str[10:11] + ":" + integer_str[12:13] + "." + float_str
        
        return ret      
        
    def eventdata_line(self,line):
        """
        EventData数据库表的填充，同时需要对message临时表进行处理
        
        """ 
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
        #CallID: 根据统计字典表进行检索与对应
        
        callid = self.callid_dict[int(line[self.item_dict[SEQUENCE]])]
        
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
            pos_eventdata[start_pos + pos] =  line[self.item_dict[message_table[pos]]]
        
        #time format
        pos_eventdata[eventdata_table["Timestamp"]] = self.time_show(line[self.item_dict[TIMESTAMP]])
        
        ################
        #Step3: 暂时不做考虑
        #pos_eventdata[eventdata_table["CallEventID"]] = ...
        
        ################
        #Step4: 时间分割
        self.split_files(line[self.item_dict[TIMESTAMP]],"eventdata_file")
        
        #############
        #step5: 最终写入
        write_line = writeStrSplit.join(pos_eventdata)
        self.current_file["eventdata_file"].write(write_line + "\n")
        
    def mr_line(self,line):
        """
        EventID = 36 时，MR数据库表
        """
        eventid = line[self.item_dict[EVENTID]]
        if eventid != "36":
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
                pos_mr[measurement_table[item]] = line[self.item_dict[origin]]   
        
        ##############
        #Step 2: 手工对应
        #CallID: 根据统计字典表进行检索与对应
        
        callid = self.callid_dict[int(line[self.item_dict[SEQUENCE]])]
        
        if callid == "":
            #print "error MR", line[self.item_dict[SEQUENCE]]
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
        
        #time format
        pos_mr[measurement_table["Timestamp"]] = self.time_show(line[self.item_dict[TIMESTAMP]])        
                
        #################
        #step 3： 暂时不做考虑
        #pos_mr[eventdata_table["MRDLossCounter"]] = ...
        #pos_mr[eventdata_table["OwnBCCH"]] = ...
        #pos_mr[eventdata_table["RxLevAntA"]] = ...
        #pos_mr[eventdata_table["RxLevAntB"]] = ...   
        #pos_mr[eventdata_table["TXID"]] = ...   
        
        #############
        #step4: 时间分割
        self.split_files(line[self.item_dict[TIMESTAMP]],"measurement_file")      
        
        #############
        #step5: 最终写入
        write_line = writeStrSplit.join(pos_mr)
        self.current_file["measurement_file"].write(write_line + "\n")        
        
    def start_line_process(self,line):
        """
        handle the first line, get the necessary key-index.
        we can ignore the differences of different eventdata
        """
        cnt = 0
        for word in line:
            self.item_dict[word] = cnt
            cnt = cnt + 1 
            
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
        tmp = filed.split(",")[2]
        cell_info = tmp.split("=")[1]
        if self.cell_dict.has_key(cell_info):
            return self.cell_dict[cell_info]
        else:
            self.cell_dict = DBInfo.loadCellInfo(CellName = cell_info)
            return self.cell_dict[cell_info]
        
        #return "7788"

    def call_dict_line(self,line):
        """
        CallInformation 字典统计临时表
        """
        call_necessay_list = [""]*len(call_fetch_correspond)     #call information 必备信息列表
        ms_individual = line[self.item_dict[MS_INDIVIDUAL]]

        if ms_individual == "":
            return
        
        #call_fetch_correspond 直接对应
        for item in call_fetch_correspond.keys():
            call_necessay_list[call_fetch_correspond[item]] = line[self.item_dict[item]] 
                
        #按照顺序添加列表
        (self.call_information[ms_individual]).append(call_necessay_list)

    def callinformation_statistics(self):
        """
        statistics: 对call_information 字典进行统计，生成callInformation表
        """
        line = []
        for ms in self.call_information.keys():
            ######################
            #Filter 
            if self.call_information[ms] == []:
                continue
            
            ######################
            #按 ms individual 进行分类
            call_list = self.call_information[ms]
            
            call_start_index = 0            #call开始索引
            call_end_index = 0              #call结束索引
            cell_last_start_index = 0       #最后一个小区开始索引
            
            record_time = ""                #计时点时间
            record_flag = False             #是否开启计时标记
            call_id = str(uuid.uuid1())          #初始时指定uuid
            
            write_line = [""]*len(callinoformation_table)
            
            ##########################################
            #遍历某个ms individual 下所有的call 信息，并进行统计
            for pos in range(0,len(call_list)):
                # 对每个位置对应的列表进行处理
                # 以event id=3 和 时间间隔 进行分割
                event_flag = call_list[pos][0]
                if event_flag != "3" and record_flag == False:
                    #没有遇到结尾且不在计时范围内
                    #基本信息刷新
                    write_line = self.basic_call(write_line, call_list[pos],call_id)
                    #索引更新
                    call_end_index,cell_last_start_index = self.update_index(call_list, pos, call_end_index, cell_last_start_index)
                        
                elif event_flag != "3" and record_flag == True:
                    #已经遇到结尾，需要判断是否在计时范围内
                    if float(call_list[pos][TIME_POS])  <= float(record_time) + split_time:
                        #在指定时间范围内
                        #基本信息刷新
                        write_line = self.basic_call(write_line, call_list[pos],call_id)
                        #索引更新
                        call_end_index,cell_last_start_index = self.update_index(call_list, pos, call_end_index, cell_last_start_index)                                              
                        
                    else:
                        #超过时间范围，需要重新进行统计,向文件中写入记录，同时清空各项标记
                        self.final_write_call(call_list, write_line, call_start_index, call_end_index, cell_last_start_index)
                        #清空记录
                        write_line = [""]*len(callinoformation_table)
                        record_flag = False
                        record_time = ""
                        #完成一次分割，更新call id
                        call_id = str(uuid.uuid1())  
                        #index 更新
                        call_start_index = pos
                        call_end_index = call_start_index
                        cell_last_start_index = call_start_index
                        #基本信息写入
                        write_line = self.basic_call(write_line, call_list[pos],call_id)
                        call_end_index,cell_last_start_index = self.update_index(call_list, pos, call_end_index, cell_last_start_index)                                              
                        
                else:
                    #event_flag = 3 clear command 指令，可能意味着要结束，
                    #此时需要更新record_time 和 record_flag
                    record_flag = True  #开始进行标记
                    record_time = call_list[pos][TIME_POS]
                    
                    #基本信息刷新
                    write_line = self.basic_call(write_line, call_list[pos],call_id)
                    #索引更新
                    call_end_index,cell_last_start_index = self.update_index(call_list, pos, call_end_index, cell_last_start_index)
            
            ##########################################
            #循环结束，写入已知结果
            self.final_write_call(call_list, write_line, call_start_index, call_end_index, cell_last_start_index)                   
                        
    def final_write_call(self,call_list,write_line,call_start_index,call_end_index,cell_last_start_index):
        """
        Final Write: call 信息统计与文本结果写入
        """
        #时间统计
        ##########
        CallStartTime = call_list[call_start_index][TIME_POS]
        CallStartTime_stand = CallStartTime.split(".")[0]
        CallStartTime_after = "0."+CallStartTime.split(".")[1]
        CallEndTime   = call_list[call_end_index][TIME_POS]
        CallEndTime_stand   = CallEndTime.split(".")[0]
        CallEndTime_after   = "0."+CallEndTime.split(".")[1] 
        start_time = float(time.mktime(time.strptime(CallStartTime_stand,"%Y%m%d%H%M%S")))  + float(CallStartTime_after)
        end_time   = float(time.mktime(time.strptime(CallEndTime_stand,"%Y%m%d%H%M%S"))) + float(CallEndTime_after)
        CallDuration  = str(end_time - start_time)
        
        #Sequence - callid 更新 --> 同一个call写对应的所有event data
        for pos in range(call_start_index,call_end_index+1):
            self.callid_dict[int(call_list[pos][SEQUENCE_POS])] = write_line[CALLID_POS]
        
        #cellid 计算
        OriginatingCellID = self.get_cellid(call_list[call_start_index][CELL_POS])
        TerminatingCellID = self.get_cellid(call_list[call_end_index][CELL_POS])
        
        #一次通话中最后一个小区的时间统计
        TimeInLastCell = str(float(call_list[call_end_index][TIME_POS]) -  float(call_list[cell_last_start_index][TIME_POS]))
        
        #目前没有处理该字段
        #CallEventID = ... 
        #TimeFromLastHOToCallEnd = ...
        #HOCount = ...
        #IntraCellHOCount = ...
        #IntraCellHOFailureCount = ...
        #HOReversionCount = ...

        #写入write_line
        write_line[CallStartTime_pos] = self.time_show(CallStartTime)
        write_line[CallEndTime_pos] = CallEndTime
        write_line[CallDuration_pos] = CallDuration
        write_line[OriginatingCellID_pos] = OriginatingCellID
        write_line[TerminatingCellID_pos] = TerminatingCellID
        write_line[TimeInLastCell_pos] = TimeInLastCell
        
        #时间分割
        self.split_files(CallStartTime,"callinformation_file") 
        
        #写入文本
        tmp = writeStrSplit.join(write_line)
        self.current_file["callinformation_file"].write(tmp + '\n')
        #self.result_callinformation_file.write(tmp + '\n')
                    
    def update_index(self,call_list,pos,call_end_index,cell_last_start_index):
        """
        Update: 时间索引和cell索引更新
        """
        #时间起始索引
        if call_list[pos][TIME_POS] >= call_list[call_end_index][TIME_POS]:
            call_end_index = pos
        else:
            call_end_index = call_end_index
        #小区信息变更
        if call_list[pos][CELL_POS] != call_list[cell_last_start_index][CELL_POS]:
            cell_last_start_index = pos
        else:
            cell_last_start_index = cell_last_start_index
            
        return (call_end_index,cell_last_start_index)
        
    def basic_call(self,write_line,call_list,call_id):
        """
        Basic: 遍历写入的条目
        """
        for item in callinoformation_table.keys():
            name  = callinformation_correspond[callinoformation_table[item]]
            if name != None:    #只替换不为空
                value = call_list[call_fetch_correspond[name]]
                write_line[item] = value if value != "" else write_line[item]           
        #id写入
        #write_line[0] = str(uuid.uuid1())
        
        #call id 写入
        write_line[0] = call_id
        return write_line
                
if __name__ == "__main__":
    print "start Time", time.ctime()
    start = time.clock()
    
    p = processStr("D:\MR\source\eventData_120419_113412.txt")    
    p.parseProcess()
    
    print "End Time", time.ctime()
    print "total time(seconds)", str(time.clock() - start)        
    