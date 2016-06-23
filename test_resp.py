#!/usr/bin/python

import threading
import time
import requests
import uuid
import time
import sys
import argparse

exitFlag = 0
cert = ('clientcert.pem','clientprivate_u.key')
url="https://cometmaster.renci.org:8443/comet-accu/rest/comet/"
dict1={}
dict2={}

parser = argparse.ArgumentParser('Parser for options of the comet Driver')
parser.add_argument('-c','--create',help='Create parameters',required=True)
parser.add_argument('-r','--read',help='Read parameters',required=True)
parser.add_argument('-u','--update',help='Update parameters',required=True)
parser.add_argument('-d','--delete',help='Delete parameters',required=True)
args=parser.parse_args()
var = ','

##create parameters**
values =  args.create.split(',')
create_counter=float(values[0])
create_delay=float(values[1])

##read parameters##
values =  args.read.split(',')
read_counter=float(values[0])
read_delay=float(values[1])

##update parameters##
values =  args.update.split(',')
update_counter=float(values[0])
update_delay=float(values[1])

##delete parameters##
values =  args.delete.split(',')
delete_counter=float(values[0])
delete_delay=float(values[1])

#curl -k -E /Users/claris/Desktop/tomcat-ssl/curlcert.pfx:password https://${hostname}:8443/comet-accu/rest/comet/readscope --data 
class readThread (threading.Thread):
    def __init__(self, name, counter,delay,lock):
        threading.Thread.__init__(self)
        self.name = name
	self.lock = lock
        self.counter = counter
	self.resource='readscope'
	self.target=url+self.resource
	self.counter=0
	self.maxi=counter
	self.delay=float(delay)
    def run(self):
	while (self.counter < self.maxi):
		self.counter = self.counter +1
		self.lock.acquire()
		if dict1:
			contextID,scopeName=dict1.popitem()
			dict1[contextID]=scopeName
			num=uuid.uuid4()
			payload={'contextType':['virtualsystems'], 'contextSubType':['iaas'], 'scopeName':[scopeName],'username':['pruth'], 'password':['pruthc4m2t'],'visibility':['actor'],'contextID':[contextID]}
			t=time.time()
			r = requests.post(self.target, data=payload,cert=cert,verify=False)
			elapsed_time=time.time() - t
			self.lock.release()
			print "Time: read " + str(elapsed_time)
			print (r.text)
		else:
			self.lock.release()
			#print "Dictionary empty in readscope"
		time.sleep(self.delay)
			

#curl -k  -E /Users/claris/Desktop/tomcat-ssl/curlcert.pfx:password https://${hostname}:8443/comet-accu/rest/comet/createscope  --data "contextType=reservations&contextSubType=iaas&scopeName=${scopeName}&scopeValue=${scopeValue}&visibility=secret&username=pruth&password=pruthc4m2t&contextID=${contextID}"
class createThread (threading.Thread):
    def __init__(self, name, counter,delay, lock):
        threading.Thread.__init__(self)
        self.name = name
	self.lock=lock
        self.counter = 0
	self.resource='createscope'
	self.target=url+self.resource
	self.maxi=counter
	self.delay=float(delay)
    def run(self):
	while(self.counter<self.maxi):
		self.counter = self.counter + 1
		contextID=str(uuid.uuid4())
		scopeName=str(uuid.uuid4())
		scopeValue=str(uuid.uuid4())
		self.lock.acquire()
		dict1[contextID]=scopeName
		dict2[scopeName]=scopeValue
		payload={'contextType':['virtualsystems'], 'contextSubType':['iaas'], 'scopeName':[scopeName],'username':['pruth'], 'password':['pruthc4m2t'],'visibility':['actor'],'contextID':[contextID],'scopeValue':[scopeValue]}
		t = time.time()
		r = requests.post(self.target, data=payload,cert=cert,verify=False)
		elapsed_time = time.time() - t
		self.lock.release()
		print "Time: create " + str(elapsed_time)
		print r.text
		time.sleep(self.delay)

def print_time(threadName, delay, counter):
    while counter:
        if exitFlag:
            threadName.exit()
        time.sleep(delay)
        print "%s: %s" % (threadName, time.ctime(time.time()))
        counter -= 1

#curl -k http://cometmaster.renci.org:8080/comet-accu/rest/comet/destroyscope --data "contextType=virtualsystems&contextID=${contextID}&scopeName=${scopeName}&username=root&password=accumuloAuth&contextSubType=iaas&visibility=myflower"
class destroyThread (threading.Thread):
    def __init__(self, name,counter, delay,lock):
        threading.Thread.__init__(self)
        self.name = name
	self.resource = 'destroyscope'
	self.lock = lock
	self.counter = 0
	self.maxi=counter
	self.delay = float(delay)
    def run(self):
	while(self.counter < self.maxi): 
		self.counter = self.counter +1
		self.lock.acquire()
		if dict1:
			#popitem() remove the key entry from the dictionary
			contextID,scopeName=dict1.popitem()
			#pop() remove the scopeName from the second dictionary
			dict2.pop(scopeName)
			payload={'contextType':['reservations'], 'contextSubType':['iaas'], 'scopeName':[scopeName],'username':['pruth'], 'password':['pruthc4m2t'],'visibility':['secret'],'contextID':[contextID]}
			target=url+self.resource
			t = time.time()
			r = requests.post(target, data=payload,cert=cert,verify=False)
			elapsed_time = time.time()-t
			self.lock.release()
			print "Time: delete " + str(elapsed_time)
			#print (r.text)
		else: 
			self.lock.release()
			#print "Dictionary is empty in destroy"
		time.sleep(self.delay)


#curl -k -E /Users/claris/Desktop/tomcat-ssl/curlcert.pfx:password https://${hostname}:8443/comet-accu/rest/comet/modifyscope --data "contextType=reservations&contextSubType=iaas&scopeValue=${scopeValue}&scopeName=${scopeName}&username=pruth&password=pruthc4m2t&visibility=secret&contextID=${contextID}

class modifyThread (threading.Thread):
    def __init__(self, name,counter, delay, lock):
        threading.Thread.__init__(self)
        self.name = name
        self.counter = 0
        self.maxi = counter
	self.lock = lock
	self.delay = float(delay)
	self.resource = 'modifyscope'
    def run(self):
	while(self.counter < self.maxi):
		self.counter = self.counter + 1
		self.lock.acquire()
		if dict1:
			#popitem() remove the key entry from the dictionary
			contextID,scopeName=dict1.popitem()
			dict1[contextID]=scopeName
			newScopeValue=uuid.uuid4()
			dict2[scopeName]=newScopeValue
			payload={'contextType':['reservations'], 'contextSubType':['iaas'], 'scopeName':[scopeName],'scopeValue':[newScopeValue],'username':['pruth'], 'password':['pruthc4m2t'],'visibility':['secret'],'contextID':[contextID]}
			target=url+self.resource
			t = time.time()
			r = requests.post(target, data=payload,cert=cert,verify=False)
			elapsed_time = time.time() - t
			self.lock.release()
			print "Time: update " + str(elapsed_time)
			print (r.text)

		else:
			self.lock.release()
			#print "Dictionary empty in modifyScope"
		time.sleep(self.delay)
			
	


lock=threading.Lock()

# Create new threads
thread1 = createThread("create_thread", create_counter, create_delay, lock)
thread2 = readThread("read_thread", read_counter, read_delay, lock)
thread3 = destroyThread("destroy_thread",delete_counter, delete_delay, lock)
thread4 = modifyThread("modify_thread",update_counter, update_delay, lock)

# Start new Threads
print "to start create"
thread1.start()
print "to start destroy"
thread2.start()
print "to start read"
thread3.start()
print "to start modify"
thread4.start()

thread1.join()
thread2.join()
thread3.join()
thread4.join()
print "Exiting Main Thread"
