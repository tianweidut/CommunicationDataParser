#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-7-2         
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

import sys,os,time

######################
#importer
encoding = sys.getfilesystemencoding()
ROOT_DIR = os.path.abspath(
                os.path.join(os.path.dirname(unicode(__file__,encoding)),os.path.pardir))
if os.path.exists(os.path.join(ROOT_DIR,'util')):
    sys.path.insert(0, ROOT_DIR)
    sys.path.insert(0, os.path.join(ROOT_DIR, 'bin'))
    sys.path.insert(0, os.path.join(ROOT_DIR, 'config'))    
    sys.path.insert(0, os.path.join(ROOT_DIR, 'util'))
else:
    raise Exception("Cannot find root dir.")

from util.process import processStr
from config.strConfig import *
from util.loadInfo import DBInfo
from optparse import OptionParser
import simplejson

def main():
    result_information = []
    load_key = {}
    
    ####################
    #command line parser
    
    parser =   OptionParser()
    parser.add_option('-f','--file',
                     action = 'store',
                     type = 'string',
                     dest = 'file_str'
                     )  
    parser.add_option('-s','--savedir',
                      action = 'store',
                      type = 'string',
                      dest = 'save_path'
                    )  
    parser.add_option('-r','--return',
                      action = 'store',
                      type = 'string',
                      dest = 'return_file')
    
    arge_test = ['-f','G:\\MR\\source\\eventData_120419_113412.txt',
                 '-s','G:\\MR\\result',
                 '-r','G:\\MR\\output.json',
                 ]
    
    (options,args) = parser.parse_args(arge_test)
    
    print options.save_path
    print options.file_str
    print options.return_file
    
    ############
    #start parsing
    file_str = options.file_str
    save_file = options.save_path            
    return_file = options.return_file 
    
    print 'Start Time:' + time.ctime()
    start = time.clock()
    
    files = []
    files.append(file_str)
    for f in files:
        if not os.path.exists(f):
            print "cannot find %s"%f
            return 
        p = processStr(filename = f,save_path = save_file)
        ret = p.parseProcess()
        print ret
        result_information.append(ret)
        break
    
    print "End Time", time.ctime()
    print "total time(seconds)", str(time.clock() - start)
    
    #####################
    #Write into JSON file
    json_object = {
                   MR_FILE:[],
                   EVENT_FILE:[],
                   CALL_FILE:[]
                   }
    for f in FILE_SETS:
        for i in range(0,MAX_TIME_SPAN+1):
            if ret[f][i]['DataStartTime'] != None and  ret[f][i]['DataStartTime'] != "":
                json_object[f].append(ret[f][i])
            else:
                break

    print json_object
    target_file = open(return_file,"w")
    simplejson.dump(json_object,target_file)
    target_file.close()
    DBInfo.close()    
    
if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    
    