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

import sys,os
import pymssql
from config.DBConfig import *
import binascii

class loadInfo(object):
    """
    load the information(key-dictionary especially) from db or file
    """
    def __init__(self):
        self.key_dict = {}
        self.conn = None
        self.current = None
        self.current_table_id = None

        self.DBConfig()
        
    def DBConfig(self):
        """"""
        print SQL_HOST,USER,PASSWORD,DATABASE
        self.conn = pymssql.connect(host=SQL_HOST,
                                    user=USER,
                                    password=PASSWORD,
                                    database=DATABASE)
        self.current = self.conn.cursor()
        
    def loadkey(self,table_id="9dd2a4c8-2766-46dc-bc08-dd7f18da6d9e"):
        """"""
        self.current_table_id = table_id
        sql_str = "select " + ColumnName +" , " + ColumnOrder \
                        + " from " + TABLE_NAME + \
                        " where " + TableID + "= '" + self.current_table_id + "'"
                      
        self.current.execute(sql_str)
        
        for item in self.current.fetchall():
            self.key_dict[item[0]] = item[1]
             
        return self.key_dict
    
    def loadkey_static(self):
        key_dict = {
                         "uuid":0,
                         "Time":1,
                         "Slot":2,
                         "ExceptionCode":3,
                         "RrcStatus":4,
                         "ConnectionStatus":5,
                         "UeRef":6,
                         "IMSI":7,
                         "cellId":8,
                         "S-RNTI":9,
                         "cellFroId ":10,
                         "connType":11,
                         "reqConnType":12,
                         "causeCode":13,
                         "Best RL in DRNC":14,
                         "sourceConntype":15,
                         "targetConnType":16,
                         "rabStatus":17
                         } 
        return key_dict       
    
    def load_cellid_dict(self,CID = "0"):
        """"""
        cell_dict = {}  
        
        self.current.execute("exec RefreshCellInfo @CID=%s",CID)
        data = self.current.fetchall()
        
        #print data
        for item in data:
            a = binascii.b2a_hex(item[cid_pos])
            cell_dict[str(item[cell_id_pos])] = a
        
        print len(cell_dict)
        self.conn.commit()        
        return cell_dict
    
    def get_causecode_value(self,casueCodeValue,exceptionCodeValue):
        self.current.execute('exec MatchException @ExceptionCode=%d, @CauseCodeString=%s',(exceptionCodeValue,casueCodeValue))
        ret = self.current.fetchall()
        return ret[0][0]
        
    def close(self):
        
        self.conn.close()
        
        
DBInfo = loadInfo()

if __name__ == "__main__":
    l = loadInfo()
    print l.load_cellid_dict("0")
    print l.loadkey()
    #print len(l.loadkey())
    
'''
        self.key_dict = {
                         "uuid":0,
                         "Date":1,
                         "Time":2,
                         "Slot":3,
                         "ExceptionCode":4,
                         "RrcStatus":5,
                         "ConnectionStatus":6,
                         "UeRef":7,
                         "IMSI":8,
                         "cellId":9,
                         "S-RNTI":10,
                         "cellFroId ":11,
                         "connType":12,
                         "reqConnType":13,
                         "causeCode":14,
                         "Best RL in DRNC":15,
                         "sourceConntype":16,
                         "targetConnType":17,
                         "rabStatus":18
                         }
'''
    

        
