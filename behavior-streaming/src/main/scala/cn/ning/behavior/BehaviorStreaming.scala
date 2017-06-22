package cn.ning.behavior

import java.util.concurrent.atomic.AtomicReference

import cn.ning.util.Config
import org.slf4j.LoggerFactory
import cn.ning.util.Constant._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}

import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{Map => MMap}
case class behavior(
    userId:String,
    day:String,
    begintime:Long,
    endtime:Long,
    pkg:String,
    activetime:Long
)
object BehaviorStreaming {
  val logger = LoggerFactory.getLogger(this.getClass().getName.stripSuffix("$"));
  val config = Config();
  val cptPath = config.getString(CONFIG_STREAMING_CPT_PATH_NING_BEHAVIOR)
  val topic = config.getString(CONFIG_KAFKA_TOPIC_NING_BEHAVIOR)
  val brokerList = config.getString(CONFIG_KAFKA_BROKER_LIST)
  val topSet = topic.split(SYMBOL_COMMA).toSet
  val groupId = config.getString(CONFIG_KAFKA_GROUPID_NING_BEHAVIOR)
  val interval = config.getLong(CONFIG_STREAMING_INTERVAL)
  val sparkConf = new SparkConf()
    .setAppName(this.getClass.getName.stripSuffix("$"))
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(cptPath,createStreamingContext)
    ssc.start()
    ssc.awaitTermination()
  }
  def createStreamingContext():StreamingContext={
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest",
      "group.id" -> groupId)
    val kafkaCluster = new KafkaCluster(kafkaParams)
    val consumerOffsets = getConsumerOffsets(kafkaCluster,groupId,topSet)
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1000")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val ssc = new StreamingContext(sc,Durations.seconds(interval))
    ssc.checkpoint(cptPath)
    val kafkaMsgDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      consumerOffsets,
      (m: MessageAndMetadata[String, String])  => m.message()
    )
    val offsetRanges = new AtomicReference[Array[OffsetRange]]()
    val msgDStreamTransform = kafkaMsgDStream.transform((rdd,time) =>{
      val _offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.set(_offsetRanges)
      logger.info(
        s"""
          |time = ${time},offsetRanges = ${_offsetRanges.mkString(";")}
        """.stripMargin)
      rdd
    })
    msgDStreamTransform.map( m =>{
      m
    }).foreachRDD(rdd =>{
      rdd.toDF("f1")
        .write
        .format("orc")
        .mode(SaveMode.Append)
        .saveAsTable("ning.ning-behavior")
      offsetToZk(kafkaCluster,offsetRanges.get(),groupId)
    })
    ssc
  }
  def offsetToZk(kafkaCluster:KafkaCluster,offsetRanges:Array[OffsetRange],groupId:String): Unit ={
    for(offset <- offsetRanges){
      val topicAndPartition = TopicAndPartition(offset.topic,offset.partition)
      val topicAndPartitionMap = Map(
        topicAndPartition,
        offset.untilOffset
      )
      kafkaCluster.setConsumerOffsets(groupId, topicAndPartitionMap)
    }
  }
  def getConsumerOffsets(kafkaCluster: KafkaCluster,groupId: String, topicSet:Set[String]) : Map[TopicAndPartition, Long] ={
    val topicandPartitions = kafkaCluster.getPartitions(topicSet).right.get
    val consumerOffsets = kafkaCluster.getConsumerOffsets(groupId,topicandPartitions) match {
      case Left(x) =>
        val consumerOffsetsLong = IMap[TopicAndPartition, Long]()
        for(topicandPartition <- topicandPartitions){
          consumerOffsetsLong(topicandPartition) = 0
        }
      case Right(offsets) =>
        offsets
    }
    null
  }
}
