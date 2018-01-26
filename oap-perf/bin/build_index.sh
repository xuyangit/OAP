#!/bin/bash

# first source config
. ./benchmark.config

hadoop fs -rm -r ${hdfsPath:7}oap-0.${oapVersionNum}.0/tpcds/tpcds${dataScale}/oap/store_sales/*.index
hadoop fs -rm -r ${hdfsPath:7}oap-0.${oapVersionNum}.0/tpcds/tpcds${dataScale}/parquet/store_sales/*.index

${sparkPath}/bin/spark-submit \
	--class IndexBuilder \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$benchmarkFormats \
	$dataScale \
	$indexFlag \
	$hdfsPath \
	$cmpBitmapAndBtree \
	1 > $indexInfoPath
