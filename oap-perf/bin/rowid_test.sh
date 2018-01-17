#!/bin/sh
bin/spark-submit \
	--class RowIdTestApp \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum
