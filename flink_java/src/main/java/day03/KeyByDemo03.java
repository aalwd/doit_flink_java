package day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import lombok.*;

/**
 * 演示keyby
 * 如果是pojo, 可以使用字段名称, 进行分区
 */
public class KeyByDemo03 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<WCBean> mapped = source.map(line -> {
            String[] split = line.split(",");
            return new WCBean(split[0], Integer.parseInt(split[1]));
        }).returns(Types.POJO(WCBean.class));


        // 按照单词进行keyby, (Tuple2<String, Integer> , 单词在Tuple2中的下标是0)
        // keyedStream的第二个泛型, 是主键key
        // 同时, key也可以是多个字段
        // flink 通过, 使用传入pojo的字段名, 来暴力反射, 获得word字段的数据
        KeyedStream<WCBean, Tuple> keyedStream = mapped.keyBy("word");


        keyedStream.print();


        env.execute();
    }





}
