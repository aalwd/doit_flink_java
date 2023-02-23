package basic.day02;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



// 使用udf的方式
public class MapDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);


        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(new StringToTupleFunction());

        map.print();


        env.execute();
    }

    public static class StringToTupleFunction extends RichMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            String[] words = value.split(",");
            return Tuple2.of(words[0], Integer.parseInt(words[1]));

        }
    }
}














