package com.linyehui.networkFlow_Analysis

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector
/**
  * @Package com.linyehui.networkFlow_Analysis
  * @author 林叶辉
  * @date 2020/10/16 22:33
  * @version V1.0
  */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    environment.setStateBackend(FsStateBackend)
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000L))
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val Stream = environment.readTextFile("E:\\Project_Location\\Flink-UserBehaviorAnalyse\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val linearray = data.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(500L)) {
        override def extractTimestamp(element: UserBehavior): Long = {element.timestamp * 1000L}
      })
      .filter(_.behavior ==  "pv")
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountWithWindow)

  }
}

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class UvCount(windowEnd: Long, count: Long)

class UvCountWithWindow extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var idSet = Set[Long]()

    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }

    out.collect(UvCount(window.getEnd, idSet.size))
  }
}