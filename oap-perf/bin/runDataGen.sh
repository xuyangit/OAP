#!/bin/sh

# first source config
. ./benchmark.config

${sparkPath}/bin/spark-submit \
	--class DataGenApp \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$dataScale \
	$testTrie \
	$dataPartitions
