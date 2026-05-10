#!/bin/bash

export SPARK_HOME=/opt/spark
export PYTHONPATH=$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
export SPARK_LOCAL_IP=127.0.0.1