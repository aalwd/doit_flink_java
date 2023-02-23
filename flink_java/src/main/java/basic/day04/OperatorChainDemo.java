package basic.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 演示算子链
 *
 * flink 程序 默认的优化策略, 就是将并行度一致,
 * 没有调用redistribute的算子连接在一起
 * 使用算子链的好处: 节省内存, 减少cpu的开销
 */
public class OperatorChainDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 在执行环境上, 全局禁用算子链 (不建议使用)
        env.disableOperatorChaining();

        DataStream<String> lines = env.socketTextStream("linux001", 8888);
        //过滤数据
        DataStream<String> filtered = lines.filter(line -> line.startsWith("ERROR"));
        //对数据进行切分
        DataStream<String> words = filtered.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                Arrays.stream(line.split(" ")).forEach(out::collect);
            }
        });
        //将单词和1组合到元组中
        DataStream<Tuple2<String, Integer>> wordAndOne = words.map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        //分组、聚合
        DataStream<Tuple2<String, Integer>> summed = wordAndOne
                .keyBy(t -> t.f0)
                .sum(1);
        //打印
        summed.print();

        env.execute();
    }
}
