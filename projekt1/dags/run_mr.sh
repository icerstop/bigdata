#!/bin/bash

if [ "$#" -ne 2 ]; then
	echo "Użycie: $0 input_dir1 output_dir3"
	exit 1
fi

INPUT_DIR=$1
OUTPUT_DIR=$2

if hadoop fs -test -d $OUTPUT_DIR; then
	hadoop fs -rm -r -f $OUTPUT_DIR
fi

hadoop jar main.jar com.example.bigdata.Main $INPUT_DIR $OUTPUT_DIR
