import urllib2
import cookielib
import pycurl
import sys
import os
from datetime import datetime, timedelta

username = 'minlanxi@gmail.com'
password = '13628345mlx'
cookie_file = 'auth.rda_ucar_edu'
loginurl = 'https://rda.ucar.edu/cgi-bin/login'
authdata = 'email=' + username + '&password=' + password + '&action=login'

url1 = 'http://rda.ucar.edu/data/ds083.2/grib1/2001/2001.01/fnl_20010101_00_00.grib1'

fnl0 = datetime(2001,1,1,0)
fnllist = []
granuleQueue = []
for i in range(100):
    fnl = fnl0 + timedelta(hours=6*i)
    fnllist.append(fnl)
    str_fnl = str(fnl)
    fid = str_fnl[0:4]+str_fnl[5:7]+str_fnl[8:10]+str_fnl[11:13]+str_fnl[14:16]
    fnlURL = ('http://rda.ucar.edu/data/ds083.2/grib1/'+str_fnl[0:4]+'/'+str_fnl[0:4]+'.'+str_fnl[5:7]+'/'+'fnl_'
          +str_fnl[0:4]+str_fnl[5:7]+str_fnl[8:10]+'_'+str_fnl[11:13]+'_'+str_fnl[14:16]+'.grib1')
    fnlFilename = 'fnl_'+str_fnl[0:4]+str_fnl[5:7]+str_fnl[8:10]+'_'+str_fnl[11:13]+'_'+str_fnl[14:16]+'.grib1'
    granuleQueue.append((fid, fnlURL, fnlFilename))

#granuleQueue.append((g.egid, granuleURL, granuleFilename))

class runManager(object):
    def __init__(self):

        self.dtStamp = dt.datetime.now()
        self.dtStamp = self.dtStamp.replace(microsecond=0)

        self.dtString = self.dtStamp.isoformat('T')
        self.dtString = self.dtString.replace("-", "_")
        self.dtString = self.dtString.replace(":", "_")

        self.logfilename = "./RDAClient_" + self.dtString + ".log"

        self.setLogFH(self.logfilename)
        self.setCmdLineArgs()

    def setLogFH(self, fname):
        try:
            self.logfh = open(fname, 'w')
        except IOError:
            EDClog.write("runManager::setLogFH\n")
            EDClog.write("\t***ERROR: Could not open ECHO Data Client Log File ({})\n".format(fname))
            raise SystemExit

    def getLogFH(self):
        return (self.logfh)



def singledownload(url,filename):

    #	for egid, url, filename in self.granuleQueue:
    c = pycurl.Curl()
    c.fp = open(filename, "w")
    
    c.setopt(pycurl.COOKIEFILE, cookie_file)
    c.setopt(c.URL, url)
    c.setopt(c.WRITEDATA, c.fp)
    c.setopt(pycurl.NOPROGRESS, 0)
    
    try:
        c.perform()
    except pycurl.error:
        print 'failed'
    else:
        print 'success'
    c.close()

singledownload(url1,'test20160405.grib1')



def multidownload(granuleQueue):
	"""
	Using PyCurl's multi-file concurrent download mechanism
	download the URL's contained in 'gQueue'.  On success, set
	status flag to 1 in 'gStatus' (gStatus is already initialized
	to 0's).

	This code is based on the Python program 'retriever-multi.py' that
	is provided with the PyCurl documentation.
	"""
	granuleStatus = {}
	concurrent_conns = 8
	queue = granuleQueue[:]
	num_urls = len(queue)

	# Pre-allocate a list of curl objects
	m = pycurl.CurlMulti()
	m.handles = []
	
	for i in range(concurrent_conns):
		c = pycurl.Curl()
		c.fp = None
		
		c.setopt(pycurl.COOKIEFILE, cookie_file)
		c.setopt(pycurl.NOPROGRESS, 0)
		c.setopt(pycurl.FOLLOWLOCATION, 1)
		c.setopt(pycurl.MAXREDIRS, 5)
		c.setopt(pycurl.CONNECTTIMEOUT, 30)
		c.setopt(pycurl.TIMEOUT, 300)
		c.setopt(pycurl.NOSIGNAL, 1)
		m.handles.append(c)

	freelist = m.handles[:]
	num_processed = 0
	
	while num_processed < num_urls:
		# If there is an url to process and a free curl object, add to multi stack
		while queue and freelist:
			fid, url, filename = queue.pop(0)
			c = freelist.pop()  # from the bottom
			c.fp = open(filename, "wb")
			
			c.setopt(pycurl.URL, url)
			c.setopt(pycurl.WRITEDATA, c.fp)
			m.add_handle(c)
			# store some info
			c.filename = filename
			c.url = url
			c.egid = fid
		# Run the internal curl state machine for the multi stack
		while 1:
			ret, num_handles = m.perform()
			if ret != pycurl.E_CALL_MULTI_PERFORM:
				break
		# Check for curl objects which have terminated, and add them to the freelist
		while 1:
			num_q, ok_list, err_list = m.info_read()
			for c in ok_list:
				c.fp.close()
				c.fp = None
				m.remove_handle(c)
				granuleStatus[fid] = 1
				EDClog.write("\tmultidownload success: %s\n" % fid)
				freelist.append(c)
			for c, errno, errmsg in err_list:
				c.fp.close()
				c.fp = None
				m.remove_handle(c)
				granuleStatus[fid] = -1
				#
				# Perhaps this is where we should remove the empty
				# local file?
				#
				EDClog.write("\tmultidownload failed: %s\n" % fid)
				freelist.append(c)
			num_processed = num_processed + len(ok_list) + len(err_list)
			if num_q == 0:
				break
		# Currently no more I/O is pending, could do something in the meantime
		# (display a progress bar, etc.).
		# We just call select() to sleep until some more data is available.
		m.select(1.0)

	# Cleanup
	for c in m.handles:
		if c.fp is not None:
			c.fp.close()
			c.fp = None
		c.close()

	m.close()

multidownload(granuleQueue)

if __name__ == '__main__':

    runMgr = runManager()
    EDClog = runMgr.getLogFH()