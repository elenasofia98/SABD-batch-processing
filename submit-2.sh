#!/bin/bash
cd /
cd ./app
$SPARK_HOME/bin/spark-submit --class "TLCMainDetachPreprocessing" --master "spark://spark:7077" ./target/SABD-batch-0.0.jar