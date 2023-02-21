package day04;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Tuple;

/**
 * 自定义分区: 按照想要的分区方式
 *
 */

public class CustomizedPartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        // 上游的分区
        SingleOutputStreamOperator<Tuple2<String, Integer>> upperStream = source.map(new RichMapFunction<String, Tuple2<String, Integer>>() {


            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value.split(",")[0], Integer.parseInt(value.split(",")[1]));
            }
        });


        // 自定义分区
        DataStream<Tuple2<String, Integer>> pa = upperStream.partitionCustom(new MyPartitioner(), t -> t.f0);


//        pa.addSink(new RichSinkFunction<String>() {
//            @Override
//            public void invoke(String value, Context context) throws Exception {
//                System.out.println(value + " 下游分区 : " + getRuntimeContext().getIndexOfThisSubtask());
//            }
//        });
        pa.print();


        env.execute();

    }

    // 自定义分区器
    public static class MyPartitioner implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            switch (key){
                case "spark" :
                    return 1;
                case "hadoop" :
                    return 2;
                case "guoze" :
                    return 3;
                case "sunheng" :
                    return 4;
                case "java" :
                    return 5;
                default:
                    return 6;

            }
        }
    }
}
