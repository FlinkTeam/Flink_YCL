../apache-flume-1.8.0-bin/bin/flume-ng agent -n agent -c ../apache-flume-1.8.0-bin/conf -f nonstandard-bcp-2-kafka.properties -Dflume.monitoring.type=http -Dflume.monitoring.port=7777
