#!/bin/bash -e
# scripts to helps formatting a json file in a condensed form
# usage: ./format.sh my_json_file
# note: does not work via stdin (json.tool doesn't seemt to support it)
python -mjson.tool ${1?} \
 | tr '\n' '\0' \
 | awk '{gsub(/, \0                                    "/,", \"")}1' \
 | awk '{gsub(/, \0                        "/,", \"")}1' \
 | awk '{gsub(/, \0            "/,", \"")}1' \
 | awk '{gsub(/{\0                                    "/,"{ \"")}1' \
 | awk '{gsub(/{\0                        "/,"{ \"")}1' \
 | awk '{gsub(/{\0            "/,"{ \"")}1' \
 | awk '{gsub(/\0                                }/," }")}1' \
 | awk '{gsub(/\0                    }/," }")}1' \
 | awk '{gsub(/\0        }/," }")}1' \
 | tr '\0' '\n'
