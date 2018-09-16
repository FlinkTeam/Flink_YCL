#!/usr/bin/env python

import urllib
import json
import os
import sched
import time
from datetime import datetime
import commands



KAFKA_HOME="/usr/local/kafka/kafka_2.11-0.10.1.1"
BOOTSTRAP_SERVERS="192.168.251.73:9092,192.168.38.36:9092,192.168.38.37:9092"
ZOOKEEPER="192.168.251.73:2181,192.168.38.36:2181,192.168.38.37:2181"
GROUP_ID="xzd"
outputtopic="t3"

FLINK_HOME="/usr/local/flink/flink-1.5.0"
PARALLELISM=8
JAR_LOCATION="/home/zhidong/ycl-flink-demo-1.0-SNAPSHOT.jar"
        
DBHOST="192.168.251.73"
DBNAME="testdb"
DBUSERNAME="root"
DBPASSWORD="root"
        
kafka_topics="{0}/bin/kafka-topics.sh --zookeeper {1} --list".format(KAFKA_HOME,ZOOKEEPER)

normal_topic="" 
highlevel_topic=""
highlevel_label="xzd"
normal_label="source_144_WA_MFORENSICS_020700"
#normal_label="source_"
rest_url="http://192.168.251.73:8081/jobs/overview"


def get_topics():
	topic_highlevel=[]
	topic_normal=[]
	topics=os.popen(kafka_topics,'r').read()
	for topic in topics.split('\n'):
		if topic.startswith(highlevel_label):
			topic_highlevel.append(topic)
		if topic.startswith(normal_label):
			topic_normal.append(topic)
	print("topic_highlevel: {0}\n".format(topic_highlevel))
	print("topic_normal: {0}\n".format(sorted(topic_normal)))
	return topic_highlevel,sorted(topic_normal)
	
def get_running_jobs():
	jobs=urllib.urlopen(rest_url)
	jobsinfo=json.loads(jobs.read()).get("jobs")
	running_joblist=[]
	for i in range(len(jobsinfo)):
		print("job name: {0}".format(jobsinfo[i].get("name")))
		print("job id: {0}".format(jobsinfo[i].get("jid")))
		print("job state: {0}\n".format(jobsinfo[i].get("state")))
		if jobsinfo[i].get("state")=="RUNNING":
			running_joblist.append(jobsinfo[i].get("name"))
	print("running jobs: {0}\n".format(running_joblist))
	return running_joblist	

def start_jobs(topics, running_jobs):
	for topic in topics[0]:
		need_start=True
		if topic in running_jobs:
			need_start=False
			break
		if need_start:
			start_job(topic)

	normal_topic=" ".join(topics[1])
	#print(normal_topic)

	if normal_topic in running_jobs:
		return
	elif normal_label not in running_jobs:
		start_job(normal_topic)
		return
		
	for topic in topics[1]:
		need_start=True
		if topic in running_jobs:
			need_start=False
			break
		if need_start:
			start_job(topic)

def start_job(intopic):
	if intopic =="":
		print("ERROR: cannot get topic infomation!!")
		return
	print("starting job: {0}".format(intopic))
	command="{0}/bin/flink run -d -p {1} {2} --input-topic \"{3}\" --output-topic {4} --bootstrap.servers {5} --zookeeper.connect {6} --group.id {7} --dbhost {8} --dbname {9} --username {10} --password {11}".format(FLINK_HOME,PARALLELISM ,JAR_LOCATION ,intopic,outputtopic,BOOTSTRAP_SERVERS,ZOOKEEPER,GROUP_ID,DBHOST,DBNAME,DBUSERNAME,DBPASSWORD)
	print(command)
	#readObj=os.popen(commond,'r')
	#print("finish starting job: {0}\n".format(readObj.read()))
	#readObj.close()
	(status,output) = commands.getstatusoutput(command)
	print("finish starting job: status: {0}\n{1}\n".format(status,output))
	time.sleep(5)


def schedule_task(inc):
	print("===========loop monitor===========")
	print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

	topiclist=get_topics()
	runningjobs=get_running_jobs()
	start_jobs(topiclist,runningjobs)

	schedule.enter(inc,0, schedule_task,(inc,))


schedule=sched.scheduler(time.time,time.sleep)
if __name__ == '__main__':

	schedule.enter(0,0, schedule_task,(120,))
	schedule.run()

