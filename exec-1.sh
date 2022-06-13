#!/bin/bash
./00-launch.sh
sleep 60
docker exec namenode hdfs dfs -rm -r "/input"
docker exec namenode hdfs dfs -rm -r "/dataset"
docker exec namenode hdfs dfs -rm -r "/query1"
docker exec namenode hdfs dfs -rm -r "/query2"
docker exec namenode rm "/output/query1.csv"
docker exec namenode rm "/output/query2.csv"
./01-loadbatch.sh
./02-make.sh
./03-launchsubmit-1.sh
./05-readoutput.sh
docker rm --force sabd-batch-processing_spark-worker-2_1
docker rm --force sabd-batch-processing_spark-worker-1_1
docker rm --force sabd-batch-processing_spark_1
docker rm --force datanode-1
docker rm --force datanode-2
docker rm --force namenode
