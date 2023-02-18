package day02;

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


    public static class MyPrintSink extends RichSinkFunction<String> {

        private int index ;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            index = getRuntimeContext().getIndexOfThisSubtask();


        }

        /**
         * sink中的数据, 来一条, 调用一次invoke方法
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
