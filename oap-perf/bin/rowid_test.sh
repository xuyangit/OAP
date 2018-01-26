#!/bin/sh

# first source config
. ./benchmark.config

bin/spark-submit \
	--class RowIdTest \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$hdfsPath
