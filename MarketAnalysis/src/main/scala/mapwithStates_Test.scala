import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
/**
  * @Package
  * @author 林叶辉
  * @date 2021/3/21 14:58
  * @version V1.0
  */
object mapwithStates_Test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(List(
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L),
      (2L, 7L),
      (2L, 4L),
      (2L, 2L)
    )).keyBy(_._1)
      .mapWithState[(Long,Long),(Long,Long)]((in:(Long,Long),count:Option[(Long,Long)]) =>
      count match {
          //底层调用了StatefulFunction
          //用一个ValueState缓存了Option[(Long,Long)]类型的计算结果
          //每个Key对应一个状态一个ValueState
        case Some(c)=>((in._1,in._2+c._2),Some(in._1,in._2+c._2))
        case None => ( (in._1, in._2), Some(in._1,in._2) )
      }
    )
      .print()
    // the printed output will be (1,4) and (1,5)

    env.execute("ExampleManagedState")

  }
}
