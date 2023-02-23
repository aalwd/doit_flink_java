package basic.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _FlatMapDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<String> mapped = ds.flatMap((String e, Collector<String> c) -> {
            String[] words = e.split(",");
            for (String word : words) {

                // 返回的数据使用collector将数据输出
                // 但是collector 中有泛型，由于使用Lambda表达式， 泛型就丢失了
                c.collect(word);
            }
        }).returns(Types.STRING);

        mapped.print();


        env.execute();
    }
}
