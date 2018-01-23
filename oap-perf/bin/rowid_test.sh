#!/bin/sh
bin/spark-submit \
	--class RowIdTest \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum
