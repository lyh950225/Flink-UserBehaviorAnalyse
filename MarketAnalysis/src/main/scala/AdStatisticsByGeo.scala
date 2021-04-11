import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Package
  * @author 林叶辉
  * @date 2020/10/18 16:43
  * @version V1.0
  */
object AdStatisticsByGeo {



  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("AdClickLog.csv")

    val adLogStream: DataStream[AdClickLog] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //黑名单过滤
    val filterBlackListStream = adLogStream
      .keyBy(logData => (logData.userId, logData.adId))
      .process(new FilterBlackListUser(100))


    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.minutes(60), Time.seconds(5))
      .aggregate(new CountAgg(), new CountResult())
//        .print()

    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist"))
      .print("black list")

    env.execute("AdStatisticsByGeo")
  }
}

//广告点击行为的样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

//输出样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)

//告警信息样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

class CountAgg() extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class CountResult() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    val windoeEnd = new Timestamp(window.getEnd).toString
    out.collect(CountByProvince(windoeEnd, key, input.head))
  }
}

class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long),AdClickLog,AdClickLog]{

  val blackListOutputTag = new OutputTag[BlackListWarning]("blacklist")
  // 保存当前用户对当前广告的点击量
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
  // 标记当前（用户，广告）作为key是否第一次发送到黑名单
  lazy val firstSent: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firstsent-state", classOf[Boolean]))
  // 保存定时器触发的时间戳，届时清空重置状态
  lazy val resetTime: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {
    val curCount = countState.value()
    // 如果是第一次处理，注册一个定时器，每天 00：00 触发清除
    if( curCount == 0 ){
      val ts = (ctx.timerService().currentProcessingTime() / (24*60*60*1000) + 1) * (24*60*60*1000)
      resetTime.update(ts)
      ctx.timerService().registerProcessingTimeTimer(ts)
    }
    // 如果计数已经超过上限，则加入黑名单，用侧输出流输出报警信息
    if (curCount >= maxCount){
      if (!firstSent.value()){
        firstSent.update(true)
        ctx.output(blackListOutputTag , BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times today.") )
      }
      //什么也不做，直接return，表示输入进来的记录就过滤掉了
      //否则就应该用Collector收集发送到下游
      return
    }

    // 如果没达到上限，点击计数加1
    countState.update(curCount + 1)
    out.collect( value )
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    //如果到达了注册定时器的时间戳
    //就将相关状态清空
    if( timestamp == resetTime.value() ){
      firstSent.clear()
      countState.clear()
    }
  }
}