package windows.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 *  多分区生成WaterMark
 *
 */
public class _17_MultiPartitionGenerateWaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //1000,spark,1
        //1000,hive,2
        //2000,spark,3
        //socket source 只有一个并行
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //经过map算子处理后，返回的DataStream，是多个并行
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tp3Stream = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.parseInt(fields[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpStreamWithWaterMark = tp3Stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                return element.f0;
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> tp2Stream = tpStreamWithWaterMark.map(new MapFunction<Tuple3<Long, String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple3<Long, String, Integer> value) throws Exception {
                return Tuple2.of(value.f1, value.f2);
            }
        });

        //先KeyBy
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tp2Stream.keyBy(t -> t.f0);

        //然后调用window方法划分KeyedWindow
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //调用WindowOperator对窗口中的数据进行运算
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);

        res.print();

        env.execute();

    }
}
