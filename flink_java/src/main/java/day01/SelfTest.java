package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SelfTest {
    public static void main(String[] args) throws Exception {
        // 引入流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从socket获得无限流
        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        // 将每行获取后,进行map 然后压平, 压平成一个word and one
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (line, collector) -> {
            String[] words = line.split("\\s+");

            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        })
                .keyBy(tp -> (String)tp.f0)
                .sum(1);

        res.print();


        env.execute();

    }
}
