package day02;


import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;

/**
 * Flink的是source按照是否有界: 有限/界的的数据流 和无限/界的数据流
 * Flink的source可以是并行执行的: 非并行的source和并行的source
 *
 * 一下是并行的无限数据流
 */
public class CustomerParallelSource01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("环境并行度" + env.getParallelism());


        DataStreamSource<String> source = env.addSource(new MyCustomSource01());

        System.out.println(source.getParallelism());
        System.out.println("mycustomsource并行度" + source.getParallelism());

        DataStreamSink<String> print = source.print();
        System.out.println("transformation 并行度: " + print.getTransformation().getParallelism());

        env.execute();
    }


    // 这个是一个并行的数据源拉取接口, 实现以后, 可以并行的,对数据源进行拉取
    public static class MyCustomSource01 implements ParallelSourceFunction<String> {
        volatile boolean flag = true;

        // run 方法是用来产生数据, 产生的数据, 使用sourceContext进行输出
        // 如果run 方法中, 没有while循环, 即run方法退出,则 该source是有限数据流
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(flag) {
                ctx.collect(UUID.randomUUID().toString());

                Thread.sleep(1000);
            }

        }


        @Override
        public void cancel() {
            // 人为停止job时, 会调用cancel方法
            // 例如点击web ui的cancel job的时候, 会调用此方法

            flag = false;

        }
    }
}
