home=$(cd `dirname $0`;cd .. ;pwd)
ssh -qt hadoop@slave6 "sh ${home}/flume/bin/stop_behavior_collect_agent.sh"
ssh -qt hadoop@slave7 "sh ${home}/flume/bin/stop_behavior_collect_agent.sh"
