package basic.day02;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomPrintSink01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);



        // 将自定义的sink添加到数据流当中,
        source.addSink(new MyPrintSink());

        env.execute();


    }


    // 自定义扩展sink方法实现类, 首先要继承RichSinkFunction(增强类) 或者SinkFunction
    public static class MyPrintSink extends RichSinkFunction<String> {

        private int index ;

        /**
         *
         * @param parameters The configuration containing the parameters attached to the contract.
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 获得当前task运行线程的运行环境的索引
            index = getRuntimeContext().getIndexOfThisSubtask();


        }

        /**
         * sink中的数据, 来一条, 调用一次invoke方法
         * 自己制定将数据传输过来是时候, 对数据做怎样的操作,
         * 不仅仅可以将数据输出, 同时也可以将数据进行存储到数据库, 或者对数据进行加工
         * 封装等 各种操作
         * @param value The input record.
         * @param context Additional context about the input record.
         * @throws Exception
         */
        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println(index + " > " + value.toUpperCase());

        }
    }
}
