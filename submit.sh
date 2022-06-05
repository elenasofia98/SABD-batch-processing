#!/bin/bash
$SPARK_HOME/bin/spark-submit --conf spark.yarn.submit.waitAppCompletion=true --class "TLCMain" --master "spark://spark:7077" target/SABD-batch-0.0.jar