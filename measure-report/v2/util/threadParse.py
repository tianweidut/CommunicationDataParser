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
import time
import subprocess
import threading
from config.strConfig import *

class threadParseWorker(threading.Thread):
    """
    """
    def __init__(self,source_filename,all_lines ,json_file_name ,save_path, max_conn = 25):
        threading.Thread.__init__(self)
        
        self.source_filename = source_filename
        self.all_lines = all_lines
        self.json_file_name = json_file_name
        self.save_path = save_path
        self.max_conn = max_conn        
                
    def run(self):
        """
        """        
        ########
        #Process
        filepath =  os.path.join(os.path.dirname(__file__),"parseProcess.py")
        cmd_str = 'python "' + filepath +'" "' + self.source_filename + '" ' \
                    + str(self.all_lines) + ' "' + self.json_file_name + '" "' + \
                    self.save_path + '" ' + str(self.max_conn) + " "
        print cmd_str
        p = subprocess.Popen(cmd_str)
        p.wait()
             
    
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
    