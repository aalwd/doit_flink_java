package windows.day05;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 不keyBy,按照ProcessingTime划分滚动窗口
 * 窗口会按照系统时间, 定期的滚动, 有数据就对窗口内的数据进行计算
 * 窗口触发后, 输出结果
 * (及时再一段时间, 没有数据产生, 窗口也是照样生成, 只不过没有数据生成而已)
 */
public class _04_ProcessingTimeNonKeyedTumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Integer> numStream = source.map(Integer::parseInt);

        // windowAll划分nokeyedWindow, 传入的是windowAssigner
        AllWindowedStream<Integer, TimeWindow> windowedStream = numStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(8)));
//        AllWindowedStream<Integer, GlobalWindow> windowedStream = numStream.timeWindowAll(Time.seconds(5));

        // 对窗口内的数据进行计算 是windowOperator
        SingleOutputStreamOperator<Integer> res = windowedStream.sum(0);

        res.print();

        env.execute();


    }
}
