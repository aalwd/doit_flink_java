package com.doit.flink.day01

import org.apache.flink.streaming.api.scala._

object StreamWordCount {

  def main(args: Array[String]): Unit = {
    /**
     * 创建spark的程序
     * 1. create  sparkcontext
     * 2. rdd
     * 3. rdd. transformation
     * 4. action
     * 5. release resources
     */


    // 创建flink 的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    // 调用evn的source方法创建DataStream(Flink的抽象数据集)
    val lines: DataStream[String] = env.socketTextStream("linux001", 8888)

    // 调用transformation
    val words = lines.flatMap(_.split("\\s+"))
    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))
    val keyed: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)
    val res = keyed.sum(1)

    // 调用sink (将结果一直运行)
    res.print()

    // 启动 一直运行
    env.execute("StreamWordCount")
  }

}
