home=$(cd `dirname $0`;cd .. ;pwd)
ssh hadoop@slave6 "sh ${home}/flume/bin/start_behavior_collect_agent.sh"
ssh hadoop@slave7 "sh ${home}/flume/bin/start_behavior_collect_agent.sh"
