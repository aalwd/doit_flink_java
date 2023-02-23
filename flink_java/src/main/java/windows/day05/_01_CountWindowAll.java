package windows.day05;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 不keyBy, 直接划分CountWindow
 */
public class _01_CountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Integer> numStream = source.map(Integer::parseInt);

        // 按照条数划分窗口
        AllWindowedStream<Integer, GlobalWindow> windowedStream = numStream.countWindowAll(5);

        // 对窗口内的数据进行计算
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();


    }
}
