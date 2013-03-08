#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-6-11         
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

#DB information

import os,sys

App_config_path = os.path.join(os.path.dirname(__file__),"App.Config")
#SQL_HOST = '192.168.20.108\sql2008r2'
#USER = 'sa'
#PASSWORD = "sql123$%^"
#DATABASE = "EricssonUEH"
SQL_HOST = ""
USER = ""
PASSWORD = ""
DATABASE = ""

#Table
Column_AllocBand   = "AllocBand"
Column_MSPower     = "MSPower"
Column_OutputPower = "OutputPower"
MSPowerDef_Table   = "Dict_MSPowerDefinition"

#storage process
STORE_PROCESS_FUNC = "RefreshCellInfo"
cell_name_pos = 0
bsc_name_pos = 1
cid_pos = 2

################
#XML DB App.conf
if SQL_HOST == "" and USER =="" and PASSWORD == "" and DATABASE =="":
    from xml.etree import ElementTree
    root_node = ElementTree.parse(App_config_path)
    
    #1. Get connectionStrings Node
    add_nodes = root_node.findall("connectionStrings/add")
    
    #2. Get 'EricssonUEHContainer' from Add node
    for add in add_nodes:
        if "EricssonMRContainer" == add.attrib["name"]:
            #Parse this line
            connectionString = (add.attrib["connectionString"]).split(";")
            for item in connectionString:
                if "data source=" in item:
                    start = item.find("data source=") 
                    start = start + len("data source=")
                    SQL_HOST = item[start:]
                elif "user id=" in item:
                    start = item.find("user id=") 
                    start = start + len("user id=")                    
                    USER = item[start:]
                elif "password=" in item:
                    start = item.find("password=") 
                    start = start + len("password=")                    
                    PASSWORD =  item[start:]
                elif "initial catalog=" in item:
                    start = item.find("initial catalog=") 
                    start = start + len("initial catalog=")                    
                    DATABASE = item[start:]
        else:
            continue

print "##########################"            
print SQL_HOST
print USER  
print PASSWORD
print DATABASE      
print "#########################"       
    
    
    
    
    
    
    
    
    
    
    
