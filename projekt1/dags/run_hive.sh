#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Użycie: $0 input_dir3 input_dir4 output_dir6"
    exit 1
fi

INPUT_DIR3=$1
INPUT_DIR4=$2
OUTPUT_DIR6=$3

if hadoop fs -test -d $OUTPUT_DIR6; then
    hadoop fs -rm -r -f $OUTPUT_DIR6
fi

hive -f hive.hql \
  --hivevar input_dir3=$INPUT_DIR3 \
  --hivevar input_dir4=$INPUT_DIR4 \
  --hivevar output_dir6=$OUTPUT_DIR6
