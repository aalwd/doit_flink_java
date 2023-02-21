package day03;


import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink的sink
 * 将数据输出到本地的文件系统,
 * 或者hdfs当中(已经过时)
 *
 */
public class WriteAsTextFileDemo01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<String> map = source.map(String::toUpperCase);
        map.setParallelism(3);

        DataStreamSink<String> stringDataStreamSink = map.writeAsText("file:\\D:\\tools\\project\\doit37_flink\\data\\sink");
        stringDataStreamSink.setParallelism(3);

        env.execute();
    }

}
