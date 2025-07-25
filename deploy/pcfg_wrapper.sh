#!/bin/bash

DIR_PATH=$(dirname "$0")

# Java runtime parameters
xmx="4G"

# read arguments, extracting the Java runtime parameters
# so that we can pass those separately to the Java binary
# (and the others to the guesser program)
skip=0
for arg do
    if [ "$1" = --max_heap ]; then
        skip=2
        xmx=$2
    fi
    if [ "$skip" -eq 0 ]; then
        set -- "$@" "$1"
    else
        skip=$(( skip - 1 ))
    fi
    shift
done
if [ "$skip" -eq 1 ]; then
    set -- "$@" -f
fi

# TODO: allow for system installed Java (or path to Java binary), without packaging it
"$DIR_PATH/jre-23/bin/java" "-Xmx$xmx" -jar "$DIR_PATH/guesser.jar" "$@"
