package basic.day03;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将KeyBy后的DataStream进行聚合
 * 将同一个enquiry, key相同的数据进行聚合()
 * reduce 方法:
 * 是同一个分区, 并且是key相同的数据可以进行聚合, 才会调用reduce方法
 */
public class ReduceDemo01 {
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


        // reuc
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceStream = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tp1, Tuple2<String, Integer> tp2) throws Exception {
                // 不使用new Tuple , 可以节省堆内存, 优化运行时间
                tp1.f1 += tp2.f1;
                return tp1;
            }
        });


        reduceStream.print();


        env.execute();
    }
}
