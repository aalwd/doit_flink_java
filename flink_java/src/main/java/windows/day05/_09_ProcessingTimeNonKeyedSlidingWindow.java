package windows.day05;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 不keyBy,按照ProcessingTime划分滚动窗口 (窗口的长度, 滑动的步长)
 *
 * 得到的是一个NoKeyedWindow, Window和WindowOperator所在的DataStream为1
 *
 */
public class _09_ProcessingTimeNonKeyedSlidingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Integer> numStream = source.map(Integer::parseInt);

        // 不keyBy,直接划分滑动窗口
        AllWindowedStream<Integer, TimeWindow> windowedStream = numStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(10)));

        SingleOutputStreamOperator<Integer> sum = windowedStream.sum(0);

        sum.print();
        env.execute();


    }
}
