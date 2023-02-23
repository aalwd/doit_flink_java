package windows.day05;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 对窗口中的数据进行聚合操作， 更加灵活的方法
 *
 * 划分窗口后， 调用reduce， sum, min, max等， 使用的是增量聚合
 * 即窗口没有出发， 输入相同的key的数据， 也会进行增量聚合， 窗口条件触发后， 输出结果
 * 优点（ 节省资源）
 *
 */
public class _06_WindowReduceDemo {
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                value1.f1 += value2.f1;
                return value1;
            }
        });


        sum.print();

        env.execute();


    }
}
