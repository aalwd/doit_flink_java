package windows.day05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * keyBy,按照ProcessingTime划分滚动窗口, 得到的窗口, 是keyedWindow
 *
 * 特点: window 和windowOrperator所在的DataStream可以是多个并行
 *
 *  在socketTextStream返回的DataStream提取EventTime生成watermark ,生成watermark只有一个分区
 *  触发条件: 当前分区中最大的EventTime - 延迟时间 >= 窗口的结束边界的闭区间
 */
public class _14_EventTimeKeyedTumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);
        //1000,spark,1
        //1000,hive,2
        //2000,spark,3


        SingleOutputStreamOperator<String> linesWithWaterMark = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = linesWithWaterMark.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value.split(",")[1], Integer.parseInt(value.split(",")[2]));
            }
        });


        KeyedStream<Tuple2<String, Integer>, Integer> res = wordCount.keyBy(e -> e.f1);
        WindowedStream<Tuple2<String, Integer>, Integer, TimeWindow> windowedStream = res.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);


        sum.print();

        env.execute();


    }
}
