'''
Created on 2012-8-1

@author: liaopengyu
'''
from util.loadInfo import DBInfo
print 'exec MatchException @ExceptionCode=%d, @CauseCodeString=%s'%(310,'dl_radio_resources_not_available')
ret = DBInfo.get_causecode_value('dl_radio_resources_not_available', 310)
print ret
ret = DBInfo.get_causecode_value('dl_radio_resources_not_available', 310)
print ret
#DBInfo.current.execute('exec MatchException @ExceptionCode=%d, @CauseCodeString=%s',(310,'dl_radio_resources_not_available'))
#print DBInfo.current.fetchall()[0][0]     
#