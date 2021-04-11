package com.linyehui.networkFlow_Analysis

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * @Package com.linyehui.networkFlow_Analysis
  * @author 林叶辉
  * @date 2020/10/16 23:17
  * @version V1.0
  */
object UvWithBloomFilter {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resourcesPath = getClass.getResource("/UserBehaviorTest.csv")
    val stream = env
      .readTextFile(resourcesPath.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60 * 60))
      .trigger(new MyTrigger)    // 自定义窗口触发规则
      .process(new UvCountWithBloom)    // 自定义窗口处理规则
  }
}

//自定义窗口触发器
class MyTrigger extends Trigger[(String,Long),TimeWindow]{
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {TriggerResult.FIRE_AND_PURGE}

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {TriggerResult.CONTINUE}

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {TriggerResult.CONTINUE}

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

class UvCountWithBloom extends ProcessWindowFunction[(String, Long),UvCount,String,TimeWindow]{

  var jedis: Jedis =  _
  var bloom : Bloom = _

  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("localhost",6379)
    bloom = new Bloom(1 << 29)
  }

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    val storeKey = context.window.getEnd.toString
    var count = 0L
    //把每个窗口对应数值存放在“count”这种表中
    if (jedis.hget("count",storeKey) != null){
      count = jedis.hget("count", storeKey).toLong
    }
    //拿到当前userId了，计算hash
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    //存放进bit位图里
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}


//手写一个布隆过滤器
class Bloom(size:Long) extends Serializable{

  private val cap = size

  def hash(value:String, seed:Int):Long =  {
    var result = 0
    for (i <- 0 until value.length){
      //最简单的hash算法，每一位字符的ASCII码值乘以seed以后，做叠加
      result = result * seed + value.charAt(i)
    }
    (cap - 1) & result
  }

}