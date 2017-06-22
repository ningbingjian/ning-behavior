package cn.ning.util

import java.util.Properties

import java.io.InputStream
import cn.ning.util.Constant._
import scala.collection.mutable.{Map => MMap}
import scala.collection.immutable.{Map => IMap}

class Config {
  val settings = MMap[String,String]()
  def load(): Unit ={
    var ips:InputStream = null
    val properties = new Properties()
    try{
      ips = this.getClass.getClassLoader.getResourceAsStream("config.properties")
      properties.load(ips)
      for(name <- properties.propertyNames()){
        settings.put(name,properties.getProperty(name))
      }
    }catch{
      case ex => throw new RuntimeException(ex)
    }finally {
        try{
          ips.close()
        }catch{
          case _=>
        }

    }
  }
  load()
  def getString(key:String):String = {
    settings(key)
  }
  def get(key:String) :String = {
    getString(key)
  }
  def getInt(key:String):Int = {
    getString(key).toInt
  }
  def getLong(key:String):Long = {
    getString(key).toLong
  }
  def getBoolean(key:String) :Boolean ={
    getString(key).toBoolean
  }
  def getStringOrElse(key:String,default:String):String = {
    settings.getOrElse(key,default)
  }
  def getIntOrElse(key:String,default:String):Int = {
    settings.getOrElse(key,default).toInt
  }
  def getLongOrElse(key:String,default:String):Long = {
    settings.getOrElse(key,default).toLong
  }
  def getBooleanOrElse(key:String,default:Boolean):Boolean = {
    if(!settings.contains(key)){
      default
    }else{
      settings(key).toBoolean
    }
  }
  def set(key:String,value:String): Unit ={
    settings(key) = value
  }


  def main(args: Array[String]): Unit = {
    println(Config().getInt(CONFIG_STREAMING_INTERVAL))
  }
}
object Config{
  def apply(): Config = new Config()
}
