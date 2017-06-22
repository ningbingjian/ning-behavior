#!/bin/sh

home=$(cd `dirname $0`; cd ..; pwd)

. ${home}/bin/common.sh

pid=`cat ${logs_home}/behavior_agent.pid | head -1`

kill ${pid}
