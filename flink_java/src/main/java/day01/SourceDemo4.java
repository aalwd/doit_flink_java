package day01;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * readTextFile基于文件的source是并行的, 但是有限数据流
 */
public class SourceDemo4 {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("执行环境的并行度: " + env.getParallelism());

        DataStreamSource<String> source = env.readFile(new TextInputFormat(new Path("data/test.txt")), "data/test.txt", FileProcessingMode.PROCESS_CONTINUOUSLY, 100);
        System.out.println("执行source的并行度: " + source.getParallelism());

        SingleOutputStreamOperator<Integer> res = source.map(e -> Integer.parseInt(e) * 10);
        System.out.println("调用完map方法返回的res的并行度: " + res.getParallelism());


        DataStreamSink<Integer> print = res.print();

        System.out.println("调用完print sink 的并行度: " + print.getTransformation().getParallelism());

        env.execute();
    }


}
