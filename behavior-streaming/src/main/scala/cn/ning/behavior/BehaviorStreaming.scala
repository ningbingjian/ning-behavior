package cn.ning.behavior

import java.util.concurrent.atomic.AtomicReference

import cn.ning.util.{Config, JsonSupport, ThreadUtil}
import org.slf4j.LoggerFactory
import cn.ning.util.Constant._
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Duration, Durations, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import cn.ning.util.JsonSupport.Json2Obj
import cn.ning.util.JsonSupport.Obj2Json

import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{Map => MMap}
import cn.ning.thread.MonitorStopThread
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.State
import org.apache.spark.streaming.Time
case class behavior(
    userId:String,
    day:String,
    begintime:Long,
    endtime:Long,
    pkg:String,
    activetime:Long
)
case class SrcBehavior(
  userId:String,
  day:String,
  begintime:Long,
  endtime:String,
  data:Seq[Data]
)
case class Data(
  pkg:String,
  activetime:Int
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
  val stopPath = config.getString(CONFIG_STREAMING_STOP_PATH_NING_BEHAVIOR)
  val stopExecutorService = ThreadUtil.newDaemonSingleThreadExecutor("stop-ning-behavior-thread")
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(cptPath,createStreamingContext)
    ssc.start()
    stopExecutorService.submit(new MonitorStopThread(stopPath,ssc))
    ssc.awaitTermination()
  }
  def createStreamingContext():StreamingContext={
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "auto.offset.reset" -> "largest",
      "group.id" -> groupId)
    val kafkaCluster = new KafkaCluster(kafkaParams)
    val consumerOffsets = getConsumerOffsets(kafkaCluster,groupId,topSet)
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName.stripSuffix("$"))
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
   // sparkConf.set("spark.streaming.kafka.maxRatePerPartition","1000")
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


    val srcBehaviorStream = kafkaMsgDStream.map{
      case s =>
        s.as[SrcBehavior]
    }

    topHotNow(srcBehaviorStream)

    topHotToNow(srcBehaviorStream)

    topHotToNow1(srcBehaviorStream)

    kafkaMsgDStream.foreachRDD(rdd =>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetToZk(kafkaCluster,offsetRanges,groupId)
    })
    ssc
  }
  def topHotToNow1(srcDS:DStream[SrcBehavior]): Unit ={
    val appNumDS = srcDS.flatMap{
      case behavior =>
        for(be <- behavior.data) yield
          (be.pkg,behavior.userId)
    }.groupByKey()
      .map{
        case (pkg,iter) =>
          (pkg,iter.toSet.size)
      }.updateStateByKey((cur:Seq[Int],pre:Option[Int]) => {
        val curSum = cur.sum

        Some(curSum + pre.getOrElse(0))
      })
    appNumDS.foreachRDD((rdd,time) =>{
      val top = rdd.takeOrdered(15)(Ordering.by(_._2 * -1))
      println("----------------------------->>topK updateStateByKey<----------------------------------------------------")
      if(top != null){
        println(top.mkString("\n"))
      }
      println("----------------------------->>topK updateStateByKey<----------------------------------------------------")

    })


  }
  def topHotToNow(srcDS:DStream[SrcBehavior]): Unit ={
    def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[Long]): Option[(String, Long)] = {
      val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
      val output = (key, sum)
      state.update(sum)
      Some(output)
    }
    val stateSpec = StateSpec.function(trackStateFunc _)
    val appNumDS = srcDS.flatMap{
      case behavior =>
        for(be <- behavior.data) yield
          (be.pkg,behavior.userId)
    }.groupByKey()
      .map{
        case (pkg,iter) =>
          (pkg,iter.toSet.size)
      }
      .mapWithState(stateSpec)
      .stateSnapshots()

    appNumDS.foreachRDD((rdd,time) =>{
      val top = rdd.takeOrdered(15)(Ordering.by(_._2 * -1))
      println("----------------------------->>topK mapWithState<----------------------------------------------------")
      if(top != null){
        println(top.mkString("\n"))
      }
      println("----------------------------->>topK mapWithState<----------------------------------------------------")

    })

  }

  def topHotNow(srcDS:DStream[SrcBehavior]): Unit ={
    val appNumDS = srcDS.flatMap{
        case behavior =>
          for( be <- behavior.data) yield
            (be.pkg,behavior.userId)

      }.groupByKey()
      .map{
       case (pkg,iter) =>
         (pkg,iter.toSet.size)
      }
    appNumDS.foreachRDD((rdd,time) =>{
      val top = rdd.takeOrdered(15)(Ordering.by(_._2 * -1))
      println("----------------------------->>topK now app<----------------------------------------------------")
      if(top != null){
        println(top.mkString("\n"))
      }
      println("----------------------------->>topK now app<----------------------------------------------------")

    })
  }

/*  def topHotNow(srcRDD:RDD[SrcBehavior]): Unit ={
    val top = srcRDD.flatMap{
        case behavior =>
          for (data <- behavior.data) yield
            (data.pkg,behavior.userId)
      }.groupByKey()
      .map{
        case (pkg,iter) =>
          (iter.toSet.size,pkg)
      }
      .takeOrdered(15)(Ordering.by(_._1 * -1))
    println("----------------------------->>topK now app<----------------------------------------------------")
    if(top != null){
      println(top.mkString("\n"))
    }
    println("----------------------------->>topK now app<----------------------------------------------------")
  }*/



  def srcBehavior(rdd:RDD[String]) = {
    rdd.map{
      case s =>
       s.as[SrcBehavior]
    }
  }
  def offsetToZk(kafkaCluster:KafkaCluster,offsetRanges:Array[OffsetRange],groupId:String): Unit ={
    for(offset <- offsetRanges){
      val topicAndPartition = TopicAndPartition(offset.topic,offset.partition)
      val topicAndPartitionMap = Map(
        topicAndPartition -> offset.untilOffset
      )
      kafkaCluster.setConsumerOffsets(groupId, topicAndPartitionMap)
    }
  }
  def getConsumerOffsets(kafkaCluster: KafkaCluster,groupId: String, topicSet:Set[String]) : Map[TopicAndPartition, Long] ={
    val topicandPartitions = kafkaCluster.getPartitions(topicSet).right.get
    val consumerOffsets = kafkaCluster.getConsumerOffsets(groupId,topicandPartitions) match {
      case Left(x) =>
        val consumerOffsetsLong = MMap[TopicAndPartition, Long]()
        for(topicandPartition <- topicandPartitions){
          consumerOffsetsLong(topicandPartition) = 0
        }
        consumerOffsetsLong.toMap
      case Right(offsets) =>
        offsets
    }
      consumerOffsets
  }
}
