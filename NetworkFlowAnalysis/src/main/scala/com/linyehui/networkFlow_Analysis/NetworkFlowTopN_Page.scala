package com.linyehui.networkFlow_Analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
  * @Package com.linyehui.networkFlow_Analysis
  * @author 林叶辉
  * @date 2020/10/12 22:34
  * @version V1.0
  */
object NetworkFlowTopN_Page {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    environment.setStateBackend(FsStateBackend)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //尝试重启的次数，每次尝试执行的间隔
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,1000L))

    val inputStream: DataStream[String] = environment.readTextFile("E:\\Project_Location\\Flink-UserBehaviorAnalyse\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    //数据清洗
    val dataStream: DataStream[ApacheLogEvent] = inputStream.map(line => {
      val fields = line.split(" ")
      val simpleFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timeStamp = simpleFormat.parse(fields(3)).getTime
      ApacheLogEvent(fields(0),"null",timeStamp,fields(5),fields(6))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })
    //开窗聚合
    val Aggregate: DataStream[PageViewCount] = dataStream
        .keyBy("url")
        .timeWindow(Time.minutes(10),Time.seconds(5))
        //这里要注意：
        //allowedLateness还会保存当前窗口的状态直至设定的时间，每来一条数据，就叠加一次状态
        //一条数据输出一次结果，这里是返回PageViewCount对象
        //但是，下游的KeyedProcessFunction函数中定义的State如果用ListState的话，就会导致同一时间窗口的同一个URL对应的PageViewCount对象有多个
        //这是不对的，我们只想保留最新的输出结果，就应该用MapState保存，每来一个PageViewCount对象，就根据URL更新一次结果
        .allowedLateness(Time.minutes(1))
        .aggregate(new PageCountAgg(),new PageCountWindowResult())

    val resultStream = Aggregate
        .keyBy(_.windowEnd) //使用样例类的属性传参的好处在于，生成的key就是一个String类型，而不是Tuple类型
        .process(new TopNPage(3))

    resultStream.print()
    environment.execute("NetworkFlowTopN_Page")
  }
}

//输入数据的样例类
case class ApacheLogEvent(ip:String, userId:String, eventTime:Long, method:String, url:String)
//聚合输出的样例类
case class PageViewCount(url:String, windowEnd:Long, count:Long)

class PageCountAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//这里的WindowFunction要记得引对包！
//import org.apache.flink.streaming.api.scala.function.WindowFunction   -》 Scala的trait
class PageCountWindowResult extends WindowFunction[Long,PageViewCount,Tuple,TimeWindow]() {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    val url: String = key.asInstanceOf[Tuple1[String]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(PageViewCount(url,windowEnd,count))
  }
}


class TopNPage(top: Int) extends KeyedProcessFunction[Long,PageViewCount,String]{

  private var mapState: MapState[String,PageViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String,PageViewCount]("List_PageCountAgg",classOf[String],classOf[PageViewCount]))
  }
  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    //将元素添加到状态列表中
    mapState.put(value.url,value)
    //注册定时器1
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1L)
    //注册定时器2
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1000L * 6)
    //allowedLateness设置的延迟时间，只有这个定时器触发才清空状态
    //不然提前清空状态的话，延迟数据无法处理
    //onTimer的逻辑中要判断触发的是哪个定时器
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //如果是定时器2，则清空状态,并退出处理
    if (timestamp == ctx.getCurrentKey + 1000L * 6) {mapState.clear();return }

    val allPageCountList: ListBuffer[PageViewCount] = ListBuffer()
    val iter = mapState.entries().iterator()
    while ( iter.hasNext ){
      allPageCountList += iter.next().getValue
    }

    //TopN
    val sortedItems = allPageCountList.sortBy(_.count)(Ordering.Long.reverse).take(top)
    //将排名信息格式化输出，方便监控显示
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val currentItem: PageViewCount = sortedItems(i)
      // e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("Top").append(i + 1).append(":")
        .append("  页面URL=").append(currentItem.url)
        .append(" 访问量=").append(currentItem.count)
        .append("\n")
    }
    result.append("====================================\n\n")

    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }
}

