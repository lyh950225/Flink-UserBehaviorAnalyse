
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Package
  * @author 林叶辉
  * @date 2020/10/19 20:55
  * @version V1.0
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val url = getClass.getResource("LoginLog.csv")
    val loginEventStream = env.readTextFile(url.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 定义匹配模式
    var loginFailPattern  = Pattern.begin[LoginEvent]("begin").where(data => data.eventType == "fail")
      .next("next").where(_.eventType == "fail")
//      .within(Time.seconds(3))
      .followedBy("follow").where(_.eventType == "success").times(3).optional
      .notNext("notNext").where(_.eventType == "fail")
      .followedBy("followedBy").where(_.eventType == "fail")
      .consecutive()  //具有连续性的模式变为严格紧邻的模式

    // 在数据流中匹配出定义好的模式
    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId),loginFailPattern)

    //// .select方法传入一个 pattern select function，当检测到定义好的模式序列时就会调用
    val loginFailStream = patternStream.select(patternSelectFun => {
      val first = patternSelectFun.getOrElse("begin",null).iterator.next()
      val second = patternSelectFun.getOrElse("next",null).iterator.next()
      Warning(first.userId,first.eventTime,second.eventTime,"login fail")
    } )

    loginFailStream.print()

    env.execute("LoginFailWithCep")
  }
}

//登录时间样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
//告警信息样例类
case class Warning(userId:Long, firstFailTime: Long, lastFailTime: Long, warnMsg: String)