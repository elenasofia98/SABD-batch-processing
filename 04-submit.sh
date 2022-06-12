#!/bin/bash
cd /app
$SPARK_HOME/bin/spark-submit --class "TLCMain" --master "spark://spark:7077" ./SABD-batch-0.0.jar