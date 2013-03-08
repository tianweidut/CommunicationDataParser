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

#字符串分割
information_split = "\t"
mo_split = ","
mo_flag = "=" 
writeStrSplit = ";"             #数据库或bulk insert 分割

#分割时间
split_time = 10.0     #Eventid = 3 后续记录时间阈值，单位秒

#标示
measurement_id = 36

#手工对应：eventData表
MS_INDIVIDUAL = "Ms individual"
SEQUENCE = "Sequence number"
MO = "MO"
TARGET_MO = "target cell"
EVENTID = "Event id"

#手工对应：Mr表
ALLOC = "Alloc Band"
MS_POWER = "ms power"
TIMESTAMP = "Timestamp"

MAX_TIME_SPAN = 24  #文件最大时间跨度

#手动对应：call表
CALLID_pos = 1
CELL_POS = 10

CallStartTime_pos = 2
CallEndTime_pos =   3
CallDuration_pos =  4
OriginatingCellID_pos = 10
TerminatingCellID_pos = 11
TimeInLastCell_pos = 23

#for write line
RxLevUL_pos = 16
RxLevDL_pos = 17 
RxQualUL_pos = 18                 
RxQualDL_pos = 19                   
TA_pos = 20

RxLevULAVG_pos = 28
RxLevDLAVG_pos = 29
RxQualULAVG_pos = 30
RxQualDLAVG_pos = 31
TAAVG_pos = 32
RxLevULMAX_pos = 33
RxLevDLMAX_pos = 34
RxQualULMAX_pos = 35
RxQualDLMAX_pos = 36
TAMAX_pos = 37

#for call fetch correspond
call_fetch_correspond_Timestamp_pos = 5
call_fetch_correspond_rxlev_ul_pos = 11
call_fetch_correspond_rxlev_dl_pos = 12
call_fetch_correspond_rxqual_ul_pos = 13
call_fetch_correspond_rxqual_dl_pos = 14
call_fetch_correspond_timing_advance_pos = 16
call_fetch_correspond_sequence_number_pos = 17


#Size
FILESIZE = 2000*100*1000     #100MB
MSSIZE = 65536
MAX_THREAD_COUNT = 10
#Files
MR_FILE = "MeasurementData"
EVENT_FILE = "EventData"
CALL_FILE = "CallInformation"

FILE_SETS = (MR_FILE,EVENT_FILE,CALL_FILE)

##folder position
#ROOT_DIR = "D:\\MR"
#RESULT_DIR = os.path.join(ROOT_DIR,'result')
#LOG_DIR = os.path.join(ROOT_DIR,'log')
#SOURCE_DIR = os.path.join(ROOT_DIR,'source')    #only test

#if not os.path.exists(ROOT_DIR):
#    os.mkdir(ROOT_DIR)

#if not os.path.exists(LOG_DIR):
#    os.mkdir(LOG_DIR)

#if not os.path.exists(SOURCE_DIR):
#    os.mkdir(SOURCE_DIR) 

#if not os.path.exists(RESULT_DIR):
#    os.mkdir(RESULT_DIR)  