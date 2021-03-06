#!/bin/sh

home=$(cd `dirname $0`; cd ..; pwd)

. ${home}/bin/common.sh

${flume_home}/bin/flume-ng agent \
--conf ${conf_home} \
-f ${conf_home}/ning-collect.conf -n a1 \
-Dflume.monitoring.type=http \
-Dflume.monitoring.port=5654 \
>> ${logs_home}/behavior_collect.cn.ning.log.producer.util.log 2>&1 &

echo $! > ${logs_home}/behavior_collect.pid
#--classpath ${lib_home}/behavior-flume-jar-with-dependencies.jar \
