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


#字符串分割
field_colums_split = "="
information_split  = ";"
information_second_split = ","  #行备选的分隔符
writeStrSplit = ";"             #数据库或bulk insert 分割
keyStrSplit = "="
keyEndSplit = ","

#关键字
sigle_ueh_key  = "(UEH_EXCEPTION)"
legal_line_key = "[20"                          #过滤非时间开头的行，以后可能会扩充去解析该二进制行
segment_key    = "Segmenting traces for key"    #处理多行数据以key作为分割符
segment_ueh_key = ":<key"                       #多行ueh中写入UEH_EXCEPTION 关键字的
segment_part_key = "part 1"                     #多行ueh中写入 关键字的

#未标示的关键字匹配
RECORD_TIME_STR = "Time"
SLOT_STR = "Slot"
CID_STR  = "cellId"
EXCEPTION_CODE = "ExceptionCode"
CAUSE_CODE = "causeCode"
KEY_STR = "key"
PART2_STR = "part 2"

#其他标示
UNFOUND_EXCEPTION_CODE = "-1"

#Max Time Span
MAX_TIME_SPAN = 10

##folder position
#ROOT_DIR = "G:\UEH"
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


