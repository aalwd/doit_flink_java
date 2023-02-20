package day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * flink 从kafka中读取数据
 * <p>
 * 现在演示的是老的api 已过时
 */
public class KafkaSource01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 指定kafka 的
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux001:9092,linux002:9092,linux003:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "G01");



        // 指定消费者, topics可以是集合 , 读取数据使用的反序列化器
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("word-count", new SimpleStringSchema(), props);
        // 添加数据源
        DataStreamSource<String> source = env.addSource(consumer);

        source.setParallelism(2); // 设置并行度, 不设置 默认是环境最大并行度


        // 调用transformation 也是默认使用环境最大并行度, 如果不设置的话,
        SingleOutputStreamOperator<String> map = source.map(String::toUpperCase);


        map.print();

        env.execute();

    }
}
