#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-5-24         
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
"""
write the log file
"""

import sys,os,time

ALL_LINES = 0
NOT_MATCH = 1
SIGNLE_UEH = 2
OTHER_UEH = 3
FILTER_NUM = 4
ERROR_NUM = 5
BINARY_NUM = 6
START_NUM = 7
PART1_NUM = 8
PART2_NUM = 9

class Logger(object):
    
    def __init__(self,source_name="",result_name="",log_name=""):
        """
        """
        self.all_lines_num  = 0     #文件中所有行数
        self.not_match_num  = 0     #未匹配的行数
        self.signle_ueh_num = 0     #type3匹配行数
        self.other_ueh_num  = 0     #type1 or type2 匹配条目数(3行标记为一个条目)
        self.filter_num     = 0     #不符合条件，直接过滤掉的数目
        self.error_num      = 0     #错误处理情况
        self.binary_num     = 0     #二进制处理（预留）
        self.start_num      = 0
        self.part1_num      = 0
        self.part2_num      = 0
        
        self.start_time = None
        self.end_time   = None
        self.total_time = 0
        
        self.source_name = source_name
        self.result_name = result_name
        self.log_name    = log_name
        self.log = None
        
        self.check_name()
        
        self.author   = "tianwei"
        self.email    = "liutianweidlut@gmail.com"
    
    def check_name(self):
        """
        """
        if self.source_name =="" or self.result_name =="" or self.log_name == "":
            print "Cannot log anything,please check according filename"
            return

    def accumulator(self,line_type):
        """
        line counter
        """
        if line_type == NOT_MATCH:
            self.not_match_num = self.not_match_num + 1
        elif line_type == SIGNLE_UEH:
            self.signle_ueh_num = self.signle_ueh_num + 1
        elif line_type == OTHER_UEH:
            self.other_ueh_num = self.other_ueh_num + 1
        elif line_type == FILTER_NUM:
            self.filter_num = self.filter_num + 1
        elif line_type == ERROR_NUM:
            self.error_num = self.error_num +1
        elif line_type == ALL_LINES:
            self.all_lines_num = self.all_lines_num +1
        elif line_type == START_NUM:
            self.start_num = self.start_num +1 
        elif line_type == PART1_NUM:
            self.part1_num = self.part1_num +1 
        elif line_type == PART2_NUM:
            self.part2_num = self.part2_num +1 
                    
    def start(self):
        """
        Log start content:start time,filename,author
        """
        
        #file open
        self.log = open(self.log_name,'w')
        self.log.write("-"*30 + '\n')
        
        self.start_time = time.ctime()
        self.total_time = time.clock()
        self.log.write(""*10 + "Basic Information" + '\n')
        self.log.write("start time: " + self.start_time + '\n')
        self.log.write("Author:" + self.author + '\n')
        self.log.write("Email:" + self.email + '\n')
        self.log.write("\n")
        self.log.write("+"*15 + '\n')
        
    def write_item(self,item):
        """
        write every item in this log file
        """
        self.log.write(item)
        
    def end(self):
        """
        Log end content:statistics
        """
        self.end_time = time.ctime()
        self.total_time = time.clock() -  self.total_time  
           
        self.log.write("\n")
        self.log.write("+"*15 + '\n')
        self.log.write(""*10 + "statistics Information" + '\n') 
        self.log.write("source file:" + self.source_name + '\n')
        self.log.write("result file:" + self.result_name + '\n')
        self.log.write("log file:" + self.log_name + '\n')
        self.log.write("\n")        
        self.log.write("start time: " + self.start_time + '\n')
        self.log.write("end time: " + self.end_time + '\n')
        self.log.write("total time:" + str(self.total_time) + '\n')  
        self.log.write("\n")
        self.log.write("all_lines_num:%d\n"%self.all_lines_num)   
        self.log.write("not_match_num:%d\n"%self.not_match_num) 
        self.log.write("signle_ueh_num:%d\n"%self.signle_ueh_num)   
        self.log.write("other_ueh_num:%d\n"%self.other_ueh_num) 
        self.log.write("filter_num:%d\n"%self.filter_num)   
        self.log.write("error_num:%d\n"%self.error_num) 
        self.log.write("Type1 or Type2:\n") 
        self.log.write("start num:%d\n"%self.start_num) 
        self.log.write("part1 num:%d\n"%self.part1_num) 
        self.log.write("part2 num:%d\n"%self.part2_num)
        self.log.write("cannot solve part1 or part2 num:%d"%(self.start_num + self.part1_num - 2* self.part2_num))          
        self.log.write("Thanks for your use. If you have any question, pleas email me directly!\n")
        self.log.close()
        
        
        