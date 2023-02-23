package windows.day05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 先keyBy, 再划分Countwindow, 属于keyedWindow
 *
 * 特点: window 和windowOrperator所在的DataStream可以是多个
 *
 * 触发条件 : 当一个组达到指定指定的条数, 这个组单独触发
 */
public class _03_CountWindow {
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

        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> res = keyedSteam.countWindow(5);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = res.sum(1);


        sum.print();

        env.execute();


    }
}
