package windows.day05;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * apply, 对窗口中的数据进行全量操作
 *
 * 是将进入到窗口内的数据先攒起来， 窗口条件触发后， 再调用apply方法对数据进行操作
 *
 *
 *
 */
public class _07_WindowApplyDemo {
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


        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedSteam.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));


        // 对窗口内的数据进行计算 是windowOperator
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            /**
             *
             * @param s The key for which this window is evaluated.
             * @param window The window that is being evaluated.
             * @param input The elements in the window being evaluated.
             * @param out A collector for emitting elements.
             * @throws Exception
             */
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                int count = 0;
                for (Tuple2<String, Integer> tp : input) {
                    count += tp.f1;
                }
                out.collect(Tuple2.of(s , count));
            }
        });


        sum.print();

        env.execute();


    }
}
