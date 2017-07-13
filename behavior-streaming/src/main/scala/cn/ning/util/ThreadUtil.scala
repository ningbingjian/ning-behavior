package cn.ning.util
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService

import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
  * Created by ning on 2017/6/26.
  */
object ThreadUtil {

  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }
}
