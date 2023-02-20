package day03;

import lombok.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 演示keyby
 * 使用
 */
public class KeyByDemo04 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<WCBean> pojo = source.map(line -> {
            String[] split = line.split(",");
            return new WCBean(split[0], Integer.parseInt(split[1]));
        }).returns(Types.POJO(WCBean.class));



        // 使用keyselector对pojo进行分区

        KeyedStream<WCBean, String> beanKeyedStream = pojo.keyBy(WCBean::getWord);



        beanKeyedStream.print();


        env.execute();
    }





}
