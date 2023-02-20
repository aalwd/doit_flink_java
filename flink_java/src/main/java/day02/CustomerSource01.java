package day02;


import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Flink的是source按照是否有界: 有限/界的的数据流 和无限/界的数据流
 * Flink的source可以是并行执行的: 非并行的source和并行的source
 * 一下是非并行的有限数据流
 */
public class CustomerSource01 {
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


    public static class MyCustomSource01 implements SourceFunction<String>{


        // run 方法是用来产生数据, 产生的数据, 使用sourceContext进行输出
        // 如果run 方法中, 没有while循环, 即run方法退出,则 该source是有限数据流
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            for (int i = 0; i < 100; i++) {
                // 输出数据
                ctx.collect("doit _ " + i);
            }

        }


        @Override
        public void cancel() {
            // 人为调用

        }
    }
}
