#!/usr/bin/env bash
nohup  >/dev/null 2>&1 &./bin/flume-ng agent -n agent -c conf -f conf/flume-conf.properties