package basic.day03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class WriteIntoKafkaDemoOldApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<String> filter = source.filter(e -> !e.startsWith("error"));


        //
        // 写入数据的序列化方式
        FlinkKafkaProducer<String> wordcount = new FlinkKafkaProducer<>("linux001:9092", "wordcount", new SimpleStringSchema());


        // 将wordcount的kafka sink 添加到数据流,
        // 数据流, 将过滤后的数据, 下沉到指定数据源
        filter.addSink(wordcount);


        env.execute();


    }
}
