#!/usr/bin/env bash

#get start config
source ./config.sh

for topic in $kafka_topics
do
	res=`expr match $topic $highlevel_label`
	if [ $res -gt 0 ];then
		highlevel_topic="${highlevel_topic} $topic"	
	fi
	res=`expr match $topic $normal_label`
	if [ $res -gt 0 ];then
		normal_topic="$topic ${normal_topic}"	
	fi
done

echo -e "hightlevel topic:\n $highlevel_topic\n"
echo -e "normallevel topic:\n $normal_topic\n"

#start high level jobs
for topic in $highlevel_topic
do

	$FLINK_HOME/bin/flink run -d -p $PARALLELISM $JAR_LOCATION --input-topic ${topic} --output-topic t3 --bootstrap.servers $BOOTSTRAP_SERVERS --zoookeeper.connect $ZOOKEEPER --group.id $GROUP_ID --dbhost $DBHOST --dbname $DBNAME --username $DBUSERNAME --password $DBPASSWORD
done

#start normal level jobs
$FLINK_HOME/bin/flink run -d -p $PARALLELISM $JAR_LOCATION --input-topic "${normal_topic}" --output-topic t3 --bootstrap.servers $BOOTSTRAP_SERVERS --zoookeeper.connect $ZOOKEEPER --group.id $GROUP_ID --dbhost $DBHOST --dbname $DBNAME --username $DBUSERNAME --password $DBPASSWORD



