package windows.day05;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有keyBy, 按照ProcessingTime划分窗口
 */
public class _11_ProcessingTimeNoKeyedSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Integer> numStream = source.map(Integer::parseInt);


        // 不是用keyBy, 划分会话窗口
        // 当时间间隔达到5s 的时候, 会将之前手机的数据进行wo计算
        AllWindowedStream<Integer, TimeWindow> windowedStream = numStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        // 调用windowOperator 对窗口进行聚合操作
        SingleOutputStreamOperator<Integer> sum = windowedStream.sum(0);

        sum.print();
        env.execute();


    }

}
