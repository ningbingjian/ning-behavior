home=$(cd `dirname $0`; cd ..; pwd)
. ${home}/bin/common.sh
nohup java -jar ${jar_home}/log-producer-jar-with-dependencies.jar &>> ${logs_home}/log-producer.log &
echo $! > ${logs_home}/log-producer.pid
