package day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.LongValueSequenceIterator;

/**
 * 演示Flink的source, 即 flink以后从哪里获取数据创建DataStream
 *
 * 该例子是演示基于socket的source, 基于集合source是无限数据流
 * 仅适用于测试环境
 */
public class SourceDemo3 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        System.out.println("执行环境的并行度: " + env.getParallelism());

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);
        System.out.println("执行source的并行度: " + source.getParallelism());

        SingleOutputStreamOperator<Integer> res = source.map(e -> Integer.parseInt(e) * 10);
        System.out.println("调用完map方法返回的res的并行度: " + res.getParallelism());


        DataStreamSink<Integer> print = res.print();

        System.out.println("调用完print sink 的并行度: " + print.getTransformation().getParallelism());

        env.execute();
    }


}
