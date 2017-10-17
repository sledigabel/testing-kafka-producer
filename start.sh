#!/bin/sh

OPTIONS=""

if [ -n "${BOOTSTRAP}" ]
then
  OPTIONS="$OPTIONS -bootstrap ${BOOTSTRAP}"
fi

if [ -n "${DRYRUN}" ]
then
  OPTIONS="$OPTIONS -dryrun"
fi

if [ -n "${DEBUG}" ]
then
  OPTIONS="$OPTIONS -debug"
fi

if [ -n "${RATE}" ]
then
  OPTIONS="$OPTIONS -rate ${RATE}"
fi

if [ -n "${DURATION}" ]
then
  OPTIONS="$OPTIONS -duration ${DURATION}"
fi

if [ -n "${TOPIC}" ]
then
  OPTIONS="$OPTIONS -topic ${TOPIC}"
fi

if [ -n "${SIZE}" ]
then
  OPTIONS="$OPTIONS -size ${SIZE}"
fi

echo /kafka-producer ${OPTIONS}
/kafka-producer ${OPTIONS}
