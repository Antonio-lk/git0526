import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //1.流的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .socketTextStream("hadoop102",9999)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }

}
