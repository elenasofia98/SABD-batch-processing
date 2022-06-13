#!/bin/bash
cd /
cd ./app
$SPARK_HOME/bin/spark-submit --class "TLCMain2" --master "spark://spark:7077" ./target/SABD-batch-0.0.jar