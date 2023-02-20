package day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用minBy, maxBy 是滚动的比较大小
 *
 * 底层也是使用的是reduce方法
 * 当数据中只有keyBy的字段, 和参与比较的字段那么max和maxBy的效果是一样的
 */
public class MaxMinByDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapped = source.map(line -> {
            String[] split = line.split(",");
            return Tuple2.of(split[0], Integer.parseInt(split[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));


        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapped.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> max = keyedStream.maxBy("f1");


        max.print();


        env.execute();
    }
}
