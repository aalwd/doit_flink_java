package basic.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
*
 * maxBy 和 max 的区别
 * maxBy如果, 数据中有参与keyby的字段, 和参与比较的字段, 还有其他的字段, maxby会返回最大值所在数据的全部字段
 */
public class MaxByDemo02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //山东省,济南市,3000
        //山东省,青岛市,2000
        //山东省,青岛市,5000
//        山东省,济南市,6000


        //河北省,廊坊市,1000
        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Tuple3<String,String,Integer>> tpStream = source.map(line -> {
            String[] split = line.split(",");
            return Tuple3.of(split[0], split[1],Integer.parseInt(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));



        // 使用keyselector对pojo进行分区

        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> tuple3Tuple2KeyedStream = tpStream.keyBy(value -> Tuple2.of(value.f0, value.f1), Types.TUPLE(Types.STRING, Types.STRING));

        // 如果相等(true) , 如果使用相等, 就返回, 第一次出现的字段
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> max = tuple3Tuple2KeyedStream.maxBy("f2");


        max.print();


        env.execute();
    }





}
