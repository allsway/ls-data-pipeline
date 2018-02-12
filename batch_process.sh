#!/bin/bash
/usr/local/spark/bin/spark-submit --jars postgresql-42.0.0.jre6.jar  ./grab_data.py config.txt
