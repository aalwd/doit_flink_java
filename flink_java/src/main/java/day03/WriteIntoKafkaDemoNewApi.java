package day03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class WriteIntoKafkaDemoNewApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<String> filter = source.filter(e -> !e.startsWith("error"));


        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("linux001")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("wc").setKeySerializationSchema(new SimpleStringSchema()).build())
                .build();

//        filter.addSink(wordcount);
        filter.sinkTo(sink);


        env.execute();



















    }
}
