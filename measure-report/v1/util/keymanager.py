#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010-2012 Tianwei Workshop
# Copyright (C) 2010-2012 Dalian University of Technology
#
# Authors: Tianwei Liu <liutianweidlut@gmail.com>
# Created: 2012-6-8         
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
Database Table dictionary information
"""
#need imported table
measurement_table = {
                     'SequenceNumber':0,
                     'Timestamp':1,
                     'CallID':2,
                     'CellID':3,
                     'TA':4,
                     'MRDLossCounter':5,
                     'RxLevelUL':6,
                     'RxLevelDL':7,
                     'RxQualUL':8,
                     'RxQualDL':9,
                     'SQI':10,
                     'SQIDL':11,
                     'FERUL':12,
                     'FERDL':13,
                     'BTSPowerReduction':14,
                     'MSPower':15,
                     'OwnBCCH':16,
                     'RxLevOwnBCCH':17,
                     'ReportingRateLevel':18,
                     'CodecType':19,
                     'CodecModeUL':20,
                     'CodecModeDL':21,
                     'NeighborCount':22,
                     'NeighborID1':23,
                     'NeighborRxLevel1':24,
                     'NeighborID2':25,
                     'NeighborRxLevel2':26,
                     'NeighborID3':27,
                     'NeighborRxLevel3':28,
                     'NeighborID4':29,
                     'NeighborRxLevel4':30,
                     'NeighborID5':31,
                     'NeighborRxLevel5':32,
                     'NeighborID6':33,
                     'NeighborRxLevel6':34,
                     'RxLevAntA':35,
                     'RxLevAntB':36,
                     'AllocBand':37,
                     'TRXCMOI':38,
                     'TXID':39,
                     'PathLossUL':40,
                     'PathLossDL':41,
                     }

#根据目前13个进行涵盖
message_table ={
                0:'channel type',
                1:'cause value',
                2:'Urgency Condition',
                3:'Extended Cause',
                4:'Repeated SACCH Activation Indicator',
                5:'Release Type',
                6:'ISHO resource allocation result',
                7:'Reason',
                8:'Traffic Case',
                9:'reestablish of layer 2 indicator',
                10:'handover failure message indicator',
                11:'IMEI TAC',
                12:'IMEI FAC',
                13:'IMEI SNR',
                14:'IMEI SVN',
                15:'Subcell type',
                16:'RR cause',
                17:'channel mode',
                18:'channel type TN',
                19:'channel type/ CDMA offset',
                20:'hopping channel',
                21:'single RF/ RF hopping channel',
                22:'channel type TCS',
                23:'locating cause',
                24:'Start Ciphering',
                25:'Ciphering Algorithm',
                26:'DHA Evaluation Data',
                27:'BPC Cap',
                28:'available channel number',
                29:'number of available channels FR',
                30:'number of available channels HR',
                31:'number of deblocked channels FR',
                32:'number of deblocked channels HR',
                33:'number of multi-rat mobiles',
                34:'Connection Type',
                35:'Priority level',
                36:'Allocated timeslot number',
                37:'Number of requested channels',
                38:'Number of allocated channels',
                39:'TRXC MOID',
                40:'target subcell',    #有疑问
                41:'DTM Flag',
                42:'Channel group',
                43:'Sub Slot',
                44:'ABIS Rate',
                45:'ABIS Config',
                46:'Time slot number',
                47:'Data Rate',
                48:'Speech version',
                49:'number of busy channels PDCH on demand',
                50:'number of busy channels PDCH dedicated',
                51:'number of busy channels HSCSD',
                52:'number of busy channels FR',
                53:'busy channel number',
                54:'channel requested speech/data indicator',
                55:'channel requested speech/data',
                56:'DHA Evaluation Data',
                57:'channel type TCS',
                }

eventdata_table = {
                   'SequenceNumber':0,
                   'Timestamp':1,
                   'CallID':2,
                   'CellID':3,
                   'Subcell':4,
                   'TargetCellID':5,
                   'ChannelTypeId':6,
                   'SpeechVersionId':7,
                   'MessageId':8,
                   'CallEventID':9,
                   'TrafficCondition':10,
                   'BSCDecision':11,
                   'LocatingCause':12,
                   'UrgencyCondition':13,
                   'ExtendedDropCause':14,
                   'message_table':15,      #message_table 表对应
                   }

callinoformation_table = {
                    0:'ID',
                    1:'CallID',
                    2:'CallStartTime',
                    3:'CallEndTime',
                    4:'CallDuration',
                    5:'TimeFromLastHOToCallEnd',
                    6:'IMEI_TAC',
                    7:'IMEI_FAC',
                    8:'IMEI_SNR',
                    9:'IMEI_SVN',
                    10:'OriginatingCellID',
                    11:'TerminatingCellID',
                    12:'HOCount',
                    13:'IntraCellHOCount',
                    14:'IntraCellHOFailureCount',
                    15:'HOReversionCount',
                    16:'RxLevUL',
                    17:'RxLevDL',
                    18:'RxQualUL',
                    19:'RxQualDL',
                    20:'TA',
                    21:'ChannelTypeId',
                    22:'CallEventID',
                    23:'TimeInLastCell',
                    24:'UrgencyCondition',
                    25:'ExtendedDropCause',
                    26:'CauseValue',
                    27:'TrafficCase',
                    }
#add more...

#corresponding fields
measurement_correspond = {
                     'SequenceNumber':'Sequence number',
                     'Timestamp':'Timestamp',
                     'CallID':None,                                     #MS Individual 
                     'CellID':None,                                     #合并计算
                     'TA':'timing advance',                 
                     'MRDLossCounter':None,                             #此项先预留，不用考虑
                     'RxLevelUL':'rxlev up-link',
                     'RxLevelDL':'rxlev down-link',
                     'RxQualUL':'rxqual up-link',
                     'RxQualDL':'rxqual down-link',
                     'SQI':'speech quality index up-link',
                     'SQIDL':'speech quality index down-link',
                     'FERUL':'fer up-link',
                     'FERDL':'fer down-link',
                     'BTSPowerReduction':'bts power reduction',
                     'MSPower':'ms power',
                     'OwnBCCH':None,                                    #此项先预留，不用考虑
                     'RxLevOwnBCCH':'rxlev own bcch',
                     'ReportingRateLevel':'reporting rate level',
                     'CodecType':'Codec type',
                     'CodecModeUL':'codec mode ul',
                     'CodecModeDL':'codec mode dl',
                     'NeighborCount':None,                              #默认为6
                     'NeighborID1':None,                                #bss neighbour cell1 (类似MO转化)
                     'NeighborRxLevel1':'rxlev bss neighbour cell1',
                     'NeighborID2':None,                                #合并计算
                     'NeighborRxLevel2':'rxlev bss neighbour cell2',
                     'NeighborID3':None,                                #合并计算
                     'NeighborRxLevel3':'rxlev bss neighbour cell3',
                     'NeighborID4':None,                                #合并计算
                     'NeighborRxLevel4':'rxlev bss neighbour cell4',
                     'NeighborID5':None,                                #合并计算
                     'NeighborRxLevel5':'rxlev bss neighbour cell5',
                     'NeighborID6':None,                                #合并计算
                     'NeighborRxLevel6':'rxlev bss neighbour cell6',
                     'RxLevAntA':None,                                  #此项先预留，不用考虑
                     'RxLevAntB':None,                                  #此项先预留，不用考虑
                     'AllocBand':'Alloc Band',
                     'TRXCMOI':'TRXC MOID',
                     'TXID':None,                           #此项先预留，不用考虑
                     'PathLossUL':None,                     #1：通过Alloc Band和ms power，在MSPowerDefinition中找到OutputPower值  
                     'PathLossDL':None,                     #2：PathLossUL = OutputPower - RxLevelUL  + 100                     
                          }

eventdata_correspond = {
                   'SequenceNumber':'Sequence number',  
                   'Timestamp':'Timestamp',
                   'CallID':None,                           #call 的uuid
                   'CellID':None,                           #合并计算
                   'Subcell':'Subcell type',
                   'TargetCellID':None,                     #target cell(类似MO转化合并计算)
                   'ChannelTypeId':'channel type',
                   'SpeechVersionId':'Speech version',
                   'MessageId':'Event id',
                   'Message':None,                          #message_table
                   'CallEventID':None,                      #....
                   'TrafficCondition':'Traffic Case',
                   'BSCDecision':None,                      #提取MO中BSC段
                   'LocatingCause':'locating cause',
                   'UrgencyCondition':'Urgency Condition',
                   'ExtendedDropCause':'Extended Cause',                     
                        }

callinformation_correspond = {
                   # 'ID':None,                              #？
                    'CallID':None,                          #uuid
                    'CallStartTime':None,                   #记录起始时间
                    'CallEndTime':None,                     #记录结束时间
                    'CallDuration':None,                    #时间差
                    'TimeFromLastHOToCallEnd':None,         #....
                    'IMEI_TAC':'IMEI TAC',
                    'IMEI_FAC':'IMEI FAC',
                    'IMEI_SNR':'IMEI SNR',
                    'IMEI_SVN':'IMEI SVN',
                    'OriginatingCellID':None,               #合并计算 第一次Event
                    'TerminatingCellID':None,               #合并计算 最后一次Event
                    'HOCount':None,                         #....
                    'IntraCellHOCount':None,                #....
                    'IntraCellHOFailureCount':None,         #....
                    'HOReversionCount':None,                #....
                    'RxLevUL':'rxlev up-link',              #是该Call的Event中，最后一个Rxlev up-link不为空的值
                    'RxLevDL':'rxlev down-link',            #同上    
                    'RxQualUL':'rxqual up-link',            #同上
                    'RxQualDL':'rxqual down-link',          #同上    
                    'TA':'timing advance',
                    'ChannelTypeId':'channel type',
                    'CallEventID':None,                     #....
                    'TimeInLastCell':None,                  #合并计算 小区分组后最后一个小区统计
                    'UrgencyCondition':'Urgency Condition',
                    'ExtendedDropCause':'Extended Cause',
                    'CauseValue':'cause value',
                    'TrafficCase':'Traffic Case',
                    }

#存入call字典表的原始信息：每个call字典只需要存储下面16条原始信息，其余信息或者暂时忽略或者通过其他方式产生、
call_fetch_correspond = {
                         'Event id':0,
                         'Traffic Case':1,
                         'cause value':2,
                         'Extended Cause':3,
                         'Urgency Condition':4,
                         'Timestamp':5,
                         'IMEI TAC':6,
                         'IMEI FAC':7,
                         'IMEI SNR':8,
                         'IMEI SVN':9,
                         'MO':10,
                         'rxlev up-link':11,
                         'rxlev down-link':12,
                         'rxqual up-link':13,
                         'rxqual down-link':14,
                         'channel type':15,
                         'timing advance':16,
                         'Sequence number':17,
                         }







