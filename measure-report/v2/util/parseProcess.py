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
        
        self.call_information = {}        

        self.callid_dict = []       #数组方式获取： 空间换时间
        
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
        for cnt in range(0,65537):
            cnt = str(cnt)
            self.call_information[cnt] = []      

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
  
        ####################################
        #call information statistics Prepare
        reader = self.readlinePartialSmall()
        line = reader.next()
        
        start = self.record_start_time("call information store")
        
        all_lines = 0
        
        start_line = True
        while line:
            all_lines += 1
            line = line.split(information_split)         
            #print line
          
            #start line process
            if start_line:
                start_line = False
                self.start_line_process(line)
            else:
                if not self.filter_empty(line): 
                    self.call_dict_line(line)
            
            #next line
            line = reader.next()
        
        self.record_finish_time(start, "call information store")      
        start = self.record_start_time("call information statistics")        
        ############################
        #Call Information Statistics
        
        if self.all_lines == 0:
            self.all_lines = all_lines
        
        self.callid_dict = [""] * (self.all_lines + 1) 
        self.callinformation_statistics()   #call 统计，获得callinformation数据库表原始值，同时生成callid
        
        self.record_finish_time(start, "call information statistics")        
        
        print "End callinformation_statistics Time",time.ctime()
        
        self.record_finish_time(start, "MR or EventData") 
        
        self.source_file.seek(0)
        reader = self.readlinePartialSmall() 
        line =  reader.next()       
        #########################
        #eventdata and mr process
        start_line = True
        while line:
            line = line.split(information_split)
            #start line process
            if start_line:
                start_line = False
            else:   #normals lines
                if not self.filter_empty(line):
                    self.normal_line(line)  
            
            line = reader.next()
        
        self.record_finish_time(start, "MR or EventData") 
            
        ######
        #close 
        #print self.result
        self.source_file.close()
        self.result_measurement_file.close()
        self.result_eventdata_file.close()
        self.result_callinformation_file.close()
        
        self.DBInfo.close()
        
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
        if "36" == line[self.item_dict[EVENTID]] or "37" == line[self.item_dict[EVENTID]]:
            #MeasurementData 表填充
            self.mr_line(line) 
        else:
            #EventData 表填充
            self.eventdata_line(line)
                
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
        #ID
        pos_eventdata[eventdata_table["ID"]] = str(uuid.uuid4())
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
        
    def mr_line(self,line):
        """
        EventID = 36 时，MR数据库表
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
        CallInformation 字典统计临时表
        """
        call_necessay_list = [""]*len(call_fetch_correspond)     #call information 必备信息列表
        ms_individual = line[self.item_dict[MS_INDIVIDUAL]]

        if ms_individual == "":
            return
        
        #call_fetch_correspond 直接对应
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
            call_id = str(uuid.uuid4())     #初始时指定uuid
            
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
                        
                #elif event_flag != "3" and record_flag == True:
                elif record_flag == True:
                    #已经遇到结尾，需要判断是否在计时范围内
                    if float(call_list[pos][call_fetch_correspond_Timestamp_pos])  <= float(record_time) + split_time:
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
                        call_id = str(uuid.uuid4())  
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
                    record_time = call_list[pos][call_fetch_correspond_Timestamp_pos]
                    
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
        CallStartTime = call_list[call_start_index][call_fetch_correspond_Timestamp_pos]
        CallStartTime_stand = CallStartTime.split(".")[0]
        CallStartTime_after = "0."+CallStartTime.split(".")[1]
        CallEndTime   = call_list[call_end_index][call_fetch_correspond_Timestamp_pos]
        CallEndTime_stand   = CallEndTime.split(".")[0]
        CallEndTime_after   = "0."+CallEndTime.split(".")[1] 
        start_time = float(time.mktime(time.strptime(CallStartTime_stand,"%Y%m%d%H%M%S")))  + float(CallStartTime_after)
        end_time   = float(time.mktime(time.strptime(CallEndTime_stand,"%Y%m%d%H%M%S"))) + float(CallEndTime_after)
        CallDuration  = str(end_time - start_time)
        
        #Sequence - callid 更新 --> 同一个call写对应的所有event data
        for pos in range(call_start_index,call_end_index+1):
            tmp = int(call_list[pos][call_fetch_correspond_sequence_number_pos])
            self.callid_dict[tmp] = write_line[CALLID_pos]
        
                
        ##处理MAX，AVG
        # ...
        # ...
        # ...
        def digital(num):
            if num.isdigit():
                return int(num)
            else: return 0
        RxLevULMAX = max(map(digital, [call_list[pos][call_fetch_correspond_rxlev_ul_pos] for pos in range(call_start_index,call_end_index+1)]))
        RxLevDLMAX = max(map(digital, [call_list[pos][call_fetch_correspond_rxlev_dl_pos] for pos in range(call_start_index,call_end_index+1)]))
        
        RxQualULMAX = max(map(digital, [call_list[pos][call_fetch_correspond_rxqual_ul_pos] for pos in range(call_start_index,call_end_index+1)]))
        RxQualDLMAX = max(map(digital, [call_list[pos][call_fetch_correspond_rxqual_dl_pos] for pos in range(call_start_index,call_end_index+1)]))
        TAMAX = max(map(digital, [call_list[pos][call_fetch_correspond_timing_advance_pos] for pos in range(call_start_index,call_end_index+1)]))
        
        lens = call_end_index + 1 - call_start_index 
        RxLevULAVG = sum(map(digital, [call_list[pos][call_fetch_correspond_rxlev_ul_pos] for pos in range(call_start_index,call_end_index+1)])) / float(lens)
        RxLevDLAVG = sum(map(digital, [call_list[pos][call_fetch_correspond_rxlev_dl_pos] for pos in range(call_start_index,call_end_index+1)])) / float(lens) 
        RxQualULAVG = sum(map(digital, [call_list[pos][call_fetch_correspond_rxqual_ul_pos] for pos in range(call_start_index,call_end_index+1)])) / float(lens)
        RxQualDLAVG = sum(map(digital, [call_list[pos][call_fetch_correspond_rxqual_dl_pos] for pos in range(call_start_index,call_end_index+1)])) / float(lens) 
        TAAVG = sum(map(digital, [call_list[pos][call_fetch_correspond_timing_advance_pos] for pos in range(call_start_index,call_end_index+1)])) / float(lens) 
        
        #cellid 计算
        OriginatingCellID = self.get_cellid(call_list[call_start_index][CELL_POS])
        TerminatingCellID = self.get_cellid(call_list[call_end_index][CELL_POS])
        
        #一次通话中最后一个小区的时间统计
        TimeInLastCell = str(float(call_list[call_end_index][call_fetch_correspond_Timestamp_pos]) -  float(call_list[cell_last_start_index][call_fetch_correspond_Timestamp_pos]))
        
        #目前没有处理该字段
        #CallEventID = ... 
        #TimeFromLastHOToCallEnd = ...
        #HOCount = ...
        #IntraCellHOCount = ...
        #IntraCellHOFailureCount = ...
        #HOReversionCount = ...

        #写入write_line
        write_line[CallStartTime_pos] = self.time_show(CallStartTime)
        write_line[CallEndTime_pos] = self.time_show(CallEndTime)
        write_line[CallDuration_pos] = CallDuration
        write_line[OriginatingCellID_pos] = OriginatingCellID
        write_line[TerminatingCellID_pos] = TerminatingCellID
        write_line[TimeInLastCell_pos] = TimeInLastCell
        
        write_line[RxLevULAVG_pos] = str(RxLevULAVG)
        write_line[RxLevDLAVG_pos] = str(RxLevDLAVG)
        write_line[RxQualULAVG_pos] = str(RxQualULAVG)
        write_line[RxQualDLAVG_pos] = str(RxQualDLAVG)
        write_line[TAAVG_pos] = str(TAAVG)
        
        write_line[RxLevULMAX_pos] = str(RxLevULMAX)
        write_line[RxLevDLMAX_pos] = str(RxLevDLMAX)
        write_line[RxQualULMAX_pos] = str(RxQualULMAX)
        write_line[RxQualDLMAX_pos] = str(RxQualDLMAX)
        write_line[TAMAX_pos] = str(TAMAX)
      
        #时间分割
        self.split_files(CallStartTime,"CallInformation") 
        
        #写入文本
        tmp = writeStrSplit.join(write_line)
        self.current_file["CallInformation"].write(tmp + '\n')
        #self.result_callinformation_file.write(tmp + '\n')
                    
    def update_index(self,call_list,pos,call_end_index,cell_last_start_index):
        """
        Update: 时间索引和cell索引更新
        """
        #时间起始索引
        if call_list[pos][call_fetch_correspond_Timestamp_pos] >= call_list[call_end_index][call_fetch_correspond_Timestamp_pos]:
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
        write_line[0] = str(uuid.uuid4())
        
        #call id 写入
        write_line[1] = call_id
        return write_line
    
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
    
    
    
    
    
    
    
    
    
    
    
    
    