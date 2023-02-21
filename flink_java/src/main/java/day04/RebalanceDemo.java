package day04;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class RebalanceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        // 上游的分区
        SingleOutputStreamOperator<String> upperStream = source.map(new RichMapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                return getRuntimeContext().getIndexOfThisSubtask() + ":上游分区 " + value;
            }
        });

        // 使用轮询进行分区, 轮询嵌套, 每个分区, 都进行轮询.
        DataStream<String> rebalanceStream = upperStream.rebalance();

        rebalanceStream.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value + " 下游分区 : " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        env.execute();

    }
}
