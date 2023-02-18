package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 用户自定义的function
public class LineToTupleFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String in, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] words = in.split("\\s+");
        for (String word : words) {
            out.collect(Tuple2.of(word, 1));
        }
    }
}
