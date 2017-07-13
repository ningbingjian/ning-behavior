home=$(cd `dirname $0`;cd .. ;pwd)
ssh  hadoop@slave8 "sh ${home}/flume/bin/start_behavior_agent.sh"
ssh  hadoop@slave9 "sh ${home}/flume/bin/start_behavior_agent.sh"
