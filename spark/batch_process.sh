#!/bin/bash
/usr/local/spark/bin/spark-submit --packages  datastax:spark-cassandra-connector:2.0.6-s_2.11  get_s3_data.py config.txt
