package com.linyehui.HotItemsAnalysis



import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
  * @Package com.linyehui
  * @author 林叶辉
  * @date 2020/10/11 21:57
  * @version V1.0
  */
object HotItems {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    environment.setStateBackend(new MemoryStateBackend())
    environment.setStateBackend(new FsStateBackend())

    //检查点配置
    environment.enableCheckpointing(1000l)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(3000L) //超时时间
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(2)  //最大并行的Checkpoint操作
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L) //两次Checkpoint操作之间，留给数据进行处理的时间
    environment.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)  //Checkpoint失败的次数
    //重启策略
    //尝试重启的次数，每次尝试执行的间隔
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000L))

    //接入文件数据源
//    val inputStream: DataStream[String] = environment.readTextFile("E:\\Project_Location\\Flink-UserBehaviorAnalyse\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    //从Kafka读取数据
    val properties = new Properties()
    properties.setProperty("boostrap.server","localhost:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serializer.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serializer.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")
    val inputStream: DataStream[String] = environment.addSource(new FlinkKafkaConsumer[String]("HotItems",new SimpleStringSchema(),properties))


    val dataStream: DataStream[UserBehavior] = inputStream.map(line => {
      val attributes = line.split(",")
      UserBehavior(attributes(0).toLong, attributes(1).toLong, attributes(2).toInt, attributes(3), attributes(4).toLong)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(500L)) {
      override def extractTimestamp(element: UserBehavior): Long = {
        element.timestamp * 1000L
      }
    })

    //对数据进行转换，过滤出PV行为，开窗聚合统计个数
    val aggStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy("ItemId")
      .timeWindow(Time.hours(1),Time.minutes(5))
      .allowedLateness(Time.seconds(5))
      .aggregate(new CountAgg, new ItemCountResult)   // CountAgg聚合函数的输出是ItemCountResult窗口函数的输入

   //再根据时间窗口进行分组，排序做TopN
    val resultStream: DataStream[String] = aggStream
        .keyBy("windowEnd")
        .process(new TopNHotItems(5))

    resultStream.print()

    environment.execute("Hot Items Analysis")

  }
}

//输入的样例类
case class UserBehavior(userId:Long, ItemId:Long, categoryId:Int, behavior:String, timestamp:Long)
//窗口聚合结果的样例类
case class ItemViewCount(ItemId:Long, windowEnd:Long, count:Long)
//增量聚合函数
class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
//窗口函数
//注意：
//数据来的时候，先一条一条的在AggregateFunction进行增量累加
//窗口计算时间到达后，触发将AggregateFunction计算的结果传给WindowFunction的行动
//所以输入不是UserBehavior，而是CountAgg的聚合结果，是一个Long
//这时此次窗口的结果通过input.iterator.next()获取，而不是全量窗口中的input.size()
class ItemCountResult extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId,windEnd,count))
  }
}

class TopNHotItems(topN:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

  lazy val listState: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("TopNState",classOf[ItemViewCount]))

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //每进来一个数据，缓存到listState中
    listState.add(value)
    //注册定时器，如果时间戳一样，那么就默认是同一个定时器，不会重复注册
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L)
  }

  //定时器触发，从状态中取数据，然后排序取TopN
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItemCountList: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    //先提取状态中的数据到一个List中
    for (itemCount <- listState.get()){
      allItemCountList += itemCount
    }

    //根据count字段进行排序，reverse逆序
    val sortedItems = allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    //清除状态
    listState.clear()

    //将排名信息格式化输出，方便监控显示
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val currentItem: ItemViewCount = sortedItems(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i + 1).append(":")
        .append("  商品ID=").append(currentItem.ItemId)
        .append("  浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")

    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}