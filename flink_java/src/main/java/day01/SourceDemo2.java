package day01;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.Arrays;
import java.util.List;

/**
 * 演示Flink的source, 即 flink以后从哪里获取数据创建DataStream
 *
 * 该例子是演示基于集合的source, 基于集合source是有限数据流
 * 仅适用于测试环境
 */
public class SourceDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("执行环境的并行度: " + env.getParallelism());

        // 调用基于集合的source


        // 基于集合的多并行的
//        DataStreamSource<Integer> dss = env.fromParallelCollection(new LongValueSequenceIterator(0, 20), Integer);
//        System.out.println("调用fromCollection方法, 返回的生成的dataStream的并行度: " + dss.getParallelism());
//
//        SingleOutputStreamOperator<Integer> res = dss.map(e -> e * 10);
//
//        System.out.println("调用完map方法返回的res的并行度: " + res.getParallelism());
//        res.print();
//        env.execute();
    }


}
