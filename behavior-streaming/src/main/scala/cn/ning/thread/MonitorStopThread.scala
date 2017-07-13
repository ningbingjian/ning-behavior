package cn.ning.thread

import cn.ning.util.DfsUtil
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}
import org.slf4j.LoggerFactory

class MonitorStopThread(val path:String,val ssc:StreamingContext) extends Runnable{
  val logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))
  override def run() = {
    var stop = false
    while(!stop){
     try{
       Thread.sleep(10000)
     }catch{
       case e:Throwable => e.printStackTrace()
     }
     if(DfsUtil.exists(path)){
       val state = ssc.getState()
       if(state == StreamingContextState.ACTIVE){
          ssc.stop(true,true)
       }else if(state == StreamingContextState.STOPPED){
          logger.info("hdfs stop path exists ,streaming be stopped")
          stop = true
       }
     }
    }
  }
}
