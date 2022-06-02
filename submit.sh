#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "TLCMain" --master "spark://spark:7077" --deploy-mode cluster target/SABD-batch-0.0.jar