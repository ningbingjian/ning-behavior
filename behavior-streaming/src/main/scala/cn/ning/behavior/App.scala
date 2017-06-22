package cn.ning.behavior

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaoshufen on 2017/6/19.
  */
object App {
  def main(args: Array[String]): Unit = {
    val mapper = new ObjectMapper()()
    mapper.registerModule(DefaultScalaModule)
  }
}
