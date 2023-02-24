package windows.day05;


import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 按照eventTime划分窗口, 不keyBy
 */
public class _13_EventTimeNonKeyedTumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        //167713920000,1

        // assignTimestampsAndWatermarks,仅仅是用来提取数据中所携带的EventTime, 用来生成WaterMark
        // 调用该方法后, 数据的格式没有改变
        SingleOutputStreamOperator<String> linesWithWaterMark = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                String s = element.split(",")[0];
                return Long.parseLong(s);
            }
        });

        SingleOutputStreamOperator<Integer> numStream = linesWithWaterMark.map(e -> Integer.parseInt(e.split(",")[1]));

        // 不keyBy, 划分EventTime
        AllWindowedStream<Integer, TimeWindow> windowStream = numStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        // 调用operator
        SingleOutputStreamOperator<Integer> res = windowStream.sum(0);


        res.print();

        env.execute();


    }
}
