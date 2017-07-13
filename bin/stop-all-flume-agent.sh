home=$(cd `dirname $0`;cd .. ;pwd)
ssh -qt hadoop@slave8 "sh ${home}/flume/bin/stop_behavior_agent.sh"
ssh -qt hadoop@slave9 "sh ${home}/flume/bin/stop_behavior_agent.sh"
