package basic.day02;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.UUID;

/**
 * Flink的是source按照是否有界: 有限/界的的数据流 和无限/界的数据流
 * Flink的source可以是并行执行的: 非并行的source和并行的source
 *
 * 一下是并行的无限数据流
 */
public class CustomerParallelSource02 {
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


    // 这个是一个并行的数据源拉取接口, 实现以后, 可以并行的,sourceContext : collector 对数据源进行拉取

    /**
     * 方法并行, 在每个taskslot中的 执行顺序:
     * open -> run(可能一直循环) -> cancel(再)
     */
    public static class MyCustomSource01 extends RichParallelSourceFunction<String> {
        boolean flag = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            System.out.println(Thread.currentThread().getName() + " run method invoked");
            while(flag) {
                ctx.collect(" uuid : " + UUID.randomUUID().toString());
                Thread.sleep(1000);
            }

        }

        @Override
        public void cancel() {
            System.out.println(Thread.currentThread().getName() + " cancel method invoked");
            flag = false;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            System.out.println(Thread.currentThread().getName() + " open method invoked");
        }

        @Override
        public void close() throws Exception {
            System.out.println(Thread.currentThread().getName() + " close method invoked");
        }
    }
}
