package basic.day02.test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class GenericTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        // 使用lamda表达式， 会自动擦除泛型类型，
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(e -> Tuple2.of(e, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));


        map.print();

        env.execute();
    }
}
