#!/bin/bash

# first source config
. ./benchmark.config

${sparkPath}/bin/spark-submit \
	--class CreateDatabase \
	--master yarn \
	--deploy-mode client \
	$benchmarkJarPath \
	$oapVersionNum \
	$benchmarkFormats \
	$dataScale \
	$testTrie \
	$hdfsPath \
	1> $fileInfoPath
