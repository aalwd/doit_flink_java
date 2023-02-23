package basic.day03;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * union将多个数据类型一样的DateStream合并在一起
 */
public class UnionDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.socketTextStream("linux001", 8888);

        DataStreamSource<String> source2 = env.socketTextStream("linux001", 9999);


        SingleOutputStreamOperator<Integer> numStream1 = source1.map(Integer::parseInt);

        numStream1.setParallelism(7);

        SingleOutputStreamOperator<Integer> numStream2= source2.map(Integer::parseInt);
        numStream2.setParallelism(8);


        // 将相同的数据类型才能union
        DataStream<Integer> union = numStream1.union(numStream2);
        int parallelism = union.getParallelism();

        System.out.println("union 后的并行度 : "+parallelism);

        SingleOutputStreamOperator<Integer> unionMapped = union.map(i -> i * 100);


        unionMapped.print();

        env.execute();
    }
}
