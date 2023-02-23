package basic.day04;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 两个算子并行度一样, 算子链被断开,
 * 使用传输数据, 为forward直传
 * 不需要跨机器, 跨网络传输数据
 * 例如 上游0号传给下游0号
 *
 */

public class ForwardDemo {
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

        // 断开前后算子的算子链
        upperStream = upperStream.disableChaining();

        // 没有起作用, 是因为flink有默认的优化策略
        DataStream<String> forward = upperStream.forward();

        forward.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value + " 下游分区 : " + getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        env.execute();

    }
}
