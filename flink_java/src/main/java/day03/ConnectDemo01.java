package day03;


import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * connect可以将两个数据类型不一样, 或一样的数据流connect到一起
 *
 * 得到的新的ConnectedStream(貌合神离) , 然后可以对内部的两个流分别进行处理
 * ***important***
 * connect后, 两个流可以共享状态,
 *
 */
public class ConnectDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.socketTextStream("linux001", 8888);

        DataStreamSource<String> source2 = env.socketTextStream("linux001", 9999);


        SingleOutputStreamOperator<Integer> numStream1 = source1.map(Integer::parseInt);

        numStream1.setParallelism(7);

        SingleOutputStreamOperator<String> stringStream = source2.map(String::toUpperCase);
        stringStream.setParallelism(8);


        // 将相同的数据类型才能union
        ConnectedStreams<Integer, String> connect = numStream1.connect(stringStream);

        // 将string和int类型的数据连接到一起
        SingleOutputStreamOperator<String> mappedConnect = connect.map(new CoMapFunction<Integer, String, String>() {

            // 可以定义共享的变量(特殊的变量, 状态)

            // 对第一个流处理的方法
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            // 对第2个流处理的方法
            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });

        mappedConnect.print();

        env.execute();
    }
}
