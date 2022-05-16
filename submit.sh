#!/bin/bash
$SPARK_HOME/bin/spark-submit --class "TLCMain" --master "local" target/SABD-batch-0.0.jar
