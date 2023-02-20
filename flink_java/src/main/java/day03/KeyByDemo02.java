package day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用lambda表达式
 * 如果dateStream中的类型是Tuple类型, 可以使用对应的Tuple的下标进行keyby
 */
public class KeyByDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapped = source.map(line -> {
            String[] split = line.split(",");
            return Tuple2.of(split[0], Integer.parseInt(split[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 使用lambda表达式
        // 按照单词进行keyby, (Tuple2<String, Integer> , 单词在Tuple2中的下标是0)
        // keyedStream的第二个泛型, 是主键key
        // 同时, key也可以是多个字段
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapped.keyBy(t -> t.f0);
//        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapped.keyBy(0,1,2);


        keyedStream.print();


        env.execute();
    }
}
