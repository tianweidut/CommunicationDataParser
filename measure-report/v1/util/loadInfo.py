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

import sys,os
import pymssql
import binascii
from config.DBConfig import *

class loadInfo(object):
    """
    load the information(key-dictionary especially) from db or file
    """
    def __init__(self):
        self.key_dict = {}
        self.cell_dict = {}
        
        self.DBConfig()
    
    def DBConfig(self):
        print SQL_HOST,USER,PASSWORD,DATABASE
        self.conn = pymssql.connect(host=SQL_HOST,
                                    user=USER,
                                    password=PASSWORD,
                                    database=DATABASE)
        self.current = self.conn.cursor()
    
    def loadkey(self):
        #此处需要载入cellFileData: Cell - CI 对应关系
        self.key_dict = {
                        
                         }
        return self.key_dict
    
    def loadOutputPower(self):
        """
        Load: 根据MSPowerDefinition获取复杂字典结构
        """
        outpower_dict = {}
        for i in range(0,6):
            outpower_dict[str(i)] = {"":""}
        
        sql_str = "select " + Column_AllocBand + " , " + Column_MSPower +" , " + Column_OutputPower + " from "  + MSPowerDef_Table 

        self.current.execute(sql_str)
        
        for item in self.current.fetchall():
            outpower_dict[item[0]][item[1]] = item[2]
        
        return outpower_dict
        
    def loadCellInfo(self,CellName = "0"):
        """
        Load:换成cell information数据库表中cellName 与 cid 对应
        """
        cell_dict = {}

        self.current.execute("exec RefreshCellInfo @CellName=%s",CellName)
        data = self.current.fetchall()
        
        #print data
        for item in data:
            cell_dict[item[cell_name_pos]] = str(item[cid_pos]) 
        
        self.conn.commit()        
        return cell_dict
    
    def close(self):
        self.conn.commit()
        self.conn.close()    
    
DBInfo = loadInfo()

if __name__ == "__main__":
    print "start"
    #print DBInfo.loadOutputPower()
    print DBInfo.loadCellInfo(CellName="LFG507B")