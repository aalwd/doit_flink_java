package basic.day02;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.Properties;

/**
 * flink 从kafka中读取数据
 * <p>
 * 现在演示的是新的api
 * 并不会自动提交偏移量
 * 需要手动设置
 */
public class KafkaSource02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用kafka的建造器 (使用的是构建者模式)
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder() // 返回建造器
                .setTopics(Collections.singletonList("word-count")) // topic
                .setGroupId("G01")// group
                .setStartingOffsets(OffsetsInitializer.earliest()) // 指定offset
                .setBootstrapServers("linux001:9092") // 指定bootstrapserver
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 指定value的反序列化器

                .setProperty("enable.auto.commit", "true") // 自动提交偏移量

                .build(); // 建造


        // 添加数据源
        // 添加数据源

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");


        SingleOutputStreamOperator<String> map = source.map(String::toUpperCase);

        map.print();


        env.execute();

    }
}
