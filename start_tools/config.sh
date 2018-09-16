#!/usr/bin/env bash

KAFKA_HOME=/usr/local/kafka/kafka_2.11-0.10.1.1
BOOTSTRAP_SERVERS=192.168.251.73:9092,192.168.38.36:9092,192.168.38.37:9092
ZOOKEEPER=192.168.251.73:2181,192.168.38.36:2181,192.168.38.37:2181
GROUP_ID=fri

FLINK_HOME=/usr/local/flink/flink-1.5.0
PARALLELISM=8
JAR_LOCATION=/home/zhidong/ycl-flink-demo-1.0-SNAPSHOT.jar

DBHOST=192.168.251.73
DBNAME=testdb
DBUSERNAME=root
DBPASSWORD=root

kafka_topics=`$KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPER --list`
normal_topic=""
highlevel_topic=""
highlevel_label="xzd"
normal_label="source_144"
