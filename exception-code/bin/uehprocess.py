#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-5-31         
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

import sys,os,subprocess,time

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
    """
    if len(sys.argv) <5:
        print 'cannot get any legal files, argv < 5 %d'%(len(sys.argv))
        return 
    """
    #python uehprocess.py -o "G:\UEH\Origin.json" -r "G:\UEH\Test.json" -f "G:\UEH\source\ECR28_0427.log"
    parser = OptionParser()
    parser.add_option('-o','--output',
                      action = 'store', 
                      type = 'string',
                      dest = 'output_str')
    parser.add_option('-f','--file',
                      action = 'store',
                      type = 'string',
                      dest = 'file_str')
    parser.add_option('-r','--return',
                      action = 'store',
                      type = 'string',
                      dest = 'return_file')
    parser.add_option('-l','--logdir',
                      action = 'store',
                      type = 'string',
                      dest = 'log_path')
    parser.add_option('-s','--savedir',
                      action = 'store',
                      type = 'string',
                      dest = 'save_path')   
    print sys.argv     
    args_test = ['-f',"D:\\ECR09_0407.log",
            '-r',"D:\UEH\Test.json",
            '-o',"D:\\724d4958-7aa6-4b34-aa26-c1c66ae2a904_Out.json",
            '-l',"D:\UEH\\log\\" ,
            '-s',"D:\UEH\\result"]
    (options,args) = parser.parse_args(args_test)
    
    print options.output_str
    print options.file_str
    print options.return_file
    print options.log_path
    print options.save_path
    #os.path.join(options.log_path)
    if not os.path.exists(options.log_path):
        os.mkdir(os.path.join(options.log_path))
    if not os.path.exists(options.save_path):
        os.mkdir(os.path.join(options.save_path))
    ############
    #JSON 格式解析
    json_val = simplejson.load(file(options.output_str))
    table_id = json_val[0].get("TableID")
    field_colums =  json_val[0].get("Columns")
    #close and delete file
    cnt = 0
    for field in field_colums:
        if field == None:
            tmp = "___" + str(cnt) + "___"
            load_key[tmp] = cnt
        else:
            keys = [key.strip(" ") for key in field.split(field_colums_split)]
#            load_key[str(field)] = cnt
            for key in keys: load_key[key] = cnt
        cnt = cnt + 1
    
    print load_key
    print table_id
    
    ############
    #start parsing
    table_id = table_id
    file_str = options.file_str
    return_file = options.return_file
    log_file = options.log_path
    save_file = options.save_path
    
    print 'Start Time:' + time.ctime()
    start = time.clock()
    
    files = []
    files.append(file_str)
    for f in files:
        if not os.path.exists(f):
            print "cannot find %s"%f
            return 
        #p = processStr(f,DBInfo.loadkey(table_id))
        p = processStr(f,load_key,table_id,log_file,save_file)
        ret = p.parseProcess()
        result_information.append(ret)
        break
    
    print "End Time", time.ctime()
    print "total time(seconds)", str(time.clock() - start)
    
    #####################
    #Write into JSON file
    json_object = []
    for i in range(0,MAX_TIME_SPAN+1):
        if ret[i]['DataStartTime'] != None and  ret[i]['DataStartTime'] != "":
            json_object.append(ret[i])
        else:
            break
    print json_object
    target_file = open(return_file,"w")
    simplejson.dump(json_object,target_file)
    target_file.close()
    DBInfo.close()
    
if __name__ == "__main__":
    main()


