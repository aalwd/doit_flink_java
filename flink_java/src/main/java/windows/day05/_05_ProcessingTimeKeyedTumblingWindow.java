package windows.day05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 不keyBy,按照ProcessingTime划分滚动窗口, 得到的窗口, 是keyedWindow
 *
 * 特点: window 和windowOrperator所在的DataStream可以是多个
 *
 *  * 窗口会按照系统时间, 定期的滚动, 有数据就对窗口内的数据进行计算
 *  * 窗口触发后, 每个组的窗口 , 都会输出结果
 */
public class _05_ProcessingTimeKeyedTumblingWindow {
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


        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedSteam.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        // 对窗口内的数据进行计算 是windowOperator
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);



        sum.print();

        env.execute();


    }
}
