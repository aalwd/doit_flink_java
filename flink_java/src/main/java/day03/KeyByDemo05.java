package day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示keyby
 * 1.每来一跳数据, 电泳一次selectChannel
 * 2.根据自定义的KeySelector, 提取出key的值
 * 3.然后计算Key的HashCode值 (可能是负数)
 * 4.使用MurmurHash计算散列( 作用: 1.将负数hashcode变成正数, 2.让散列相对更加均匀 , 但解决不了hash碰撞
 * 5.计算keyGroupId ( % 最大并行度)
 * 6.计算下游分区Index ( * 下游分区数 / 最大并行度)
 */
public class KeyByDemo05 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //山东省,济南市,3000
        //山东省,青岛市,2000
        //河北省,廊坊市,1000
        //河北省,石家庄,1000
//        山东省,青岛市,2888
        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Tuple3<String,String,Integer>> tpStream = source.map(line -> {
            String[] split = line.split(",");
            return Tuple3.of(split[0], split[1],Integer.parseInt(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));



        // 使用keyselector对pojo进行分区

        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> tuple3Tuple2KeyedStream = tpStream.keyBy(value -> Tuple2.of(value.f0, value.f1), Types.TUPLE(Types.STRING, Types.STRING));


        tuple3Tuple2KeyedStream.print();


        env.execute();
    }





}
