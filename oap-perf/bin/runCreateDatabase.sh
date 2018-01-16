#!/bin/bash

# first source config
. ./benchmark.config


${sparkPath}/bin/spark-submit \
	--class CreateDatabaseApp \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$dataScale \
	$testTrie
	1> $tableInfo
