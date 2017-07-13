home=$(cd `dirname $0`;cd .. ;pwd)
ssh -qt hadoop@slave8 "sh ${home}/cn.ning.log.producer.util.log-producer/bin/stop-cn.ning.log.producer.util.log-producer.sh"
ssh -qt hadoop@slave9 "sh ${home}/cn.ning.log.producer.util.log-producer/bin/stop-cn.ning.log.producer.util.log-producer.sh"
