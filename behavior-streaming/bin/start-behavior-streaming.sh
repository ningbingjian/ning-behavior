home=`cd dirname $0;cd ..;pwd`
. ${home}/bin/common.sh
#hdfs dfs -rmr  /user/ning/ning-behavior/streaming/cpt
#hdfs dfs -rmr /user/ning/ning-behavior/streaming/stop
echo ${spark_home}
#${spark_home}/bin/spark-submit --master yarn --num-executors 2  --executor-memory 1G --class cn.ning.behavior.BehaviorStreaming    --jars $(echo /hwdata/ning/app/ning-behavior/behavior-streaming/lib/* |tr ' ' ',')  /hwdata/ning/app/ning-behavior/behavior-streaming/behavior-streaming-1.0.jar &> /hwdata/ning/app/ning-behavior/behavior-streaming/logs/cn.ning.log.producer.util.log.cn.ning.log.producer.util.log &
