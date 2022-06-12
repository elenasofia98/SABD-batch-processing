#!/bin/bash
docker exec -it namenode hdfs dfs -getmerge /query1 /output/query1.csv
docker exec -it namenode hdfs dfs -getmerge /query2 /output/query2.csv
