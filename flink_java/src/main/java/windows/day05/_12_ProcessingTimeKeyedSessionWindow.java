package windows.day05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * keyBy,按照Session划分滚动窗口, 得到的窗口, 是keyedWindow
 *
 * 特点: window 和windowOrperator所在的DataStream可以是多个
 *
 *  keyed ProcessingTimeSessionWindow触发时机 :
 *  当前系统时间 - 进入到某一个组内最后一条数据的时间 > 指定的时间间隔, 那么这个组单独触发
 */
public class _12_ProcessingTimeKeyedSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value.split(",")[0], Integer.parseInt(value.split(",")[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedSteam = map.keyBy(tp -> tp.f0);


        // window中传入windowAssigner 窗口分配器
        // 按照自定义方式来调用窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedSteam.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));


        // 对窗口内的数据进行计算 是windowOperator
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);



        sum.print();

        env.execute();


    }
}
