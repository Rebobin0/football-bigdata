#!/bin/bash

export SPARK_HOME=/opt/spark
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 pyspark-shell"