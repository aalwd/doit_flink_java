package basic.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        // 创建flink流执行环境
        StreamExecutionEnvironment env  = StreamExecutionEnvironment.getExecutionEnvironment();

        // 调用env的source方法, 创建DataStream
        DataStreamSource<String> lines = env.socketTextStream("linux001", 8888);

        // 调用transformation
        SingleOutputStreamOperator<String> wordsAndOne = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split("\\s+");
                for (String word : words) {
                    // 使用collector输出数据
                    collector.collect(word);
                }
            }
        });
        // 将单词和1组合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordandone = wordsAndOne.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        // 按照可以进行keyby , 就是将key相同的数据通过网络传输到同一台机器的分区中, (底层使用hash方法分区)

        KeyedStream<Tuple2<String, Integer>, String> stream = wordandone.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });


        // 将同一个分区, key相同的value进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream.sum(1);


        sum.print();


        // 启动 一直执行
        env.execute();
    }
}
