#!/bin/bash

# first source config
. ./benchmark.config

# prepare hdfs path
hadoop fs -mkdir -p ${hdfsPath:7}oap-0.${oapVersionNum}.0/tpcds/tpcds${scale}/parquet
hadoop fs -mkdir -p ${hdfsPath:7}oap-0.${oapVersionNum}.0/tpcds/tpcds${scale}/oap

# run preparation workflow
./gen_data.sh
./create_database.sh
./build_index.sh