package basic.day03;

import basic.day01.LineToTupleFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class WriteInRedisDemo01 {
    public static void main(String[] args) throws Exception {
        //创建flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //调用env的source方法创建DataStream
        DataStreamSource<String> lines = env.socketTextStream("linux001", 8888);

        // 使用FlinkJedisPoolConfig中的构造器, 来建立sink
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("linux001")
                .setPort(6379)
                .setDatabase(0)
                .build();

        //调用Transformation
        //切分压平，同时将单词和1组合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = lines.flatMap(new LineToTupleFunction())
                .keyBy(t -> t.f0)
                .sum(1);


        RedisSink<Tuple2<String, Integer>> sink = new RedisSink<>(config, new WordMapper());


        sum.addSink(sink);
        env.execute();


    }


    // 需要实现RedisMapper来添加数据,
    // 需要在数据传入的时候, 指定数据的key类型以及值, 和value的类型和数据
    public static class WordMapper implements RedisMapper<Tuple2<String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {

            return new RedisCommandDescription(RedisCommand.HSET, "mywc");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> tp) {
            return tp.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> tp) {
            return tp.f1.toString();
        }
    }
}
