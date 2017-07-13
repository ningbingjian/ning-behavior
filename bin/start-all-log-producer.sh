home=$(cd `dirname $0`;cd .. ;pwd)
ssh  hadoop@slave8 "sh ${home}/cn.ning.log.producer.util.log-producer/bin/start-cn.ning.log.producer.util.log-producer.sh"
ssh  hadoop@slave9 "sh ${home}/cn.ning.log.producer.util.log-producer/bin/start-cn.ning.log.producer.util.log-producer.sh"
