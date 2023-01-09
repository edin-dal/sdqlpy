#!/bin/sh

# echo off > /sys/devices/system/cpu/smt/control

caller_path="$1/"
current_path=$(pwd)

echo "#### Code generation started..."
python3 $caller_path"lib/sdql_compiler.py" $2 1 $3 $caller_path
python3 $caller_path"lib/fast_dict_generator.py" $2 $caller_path 
echo "#### Code generation done."

echo "#### Compilation started..."
python3 $current_path"/fast_dict_setup.py" install
echo "#### Compilation done."