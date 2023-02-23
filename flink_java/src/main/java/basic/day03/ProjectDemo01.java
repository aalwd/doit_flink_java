package basic.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * project,
 * 只能用于元祖, 代表映射元祖中某些你想要的字段
 */
public class ProjectDemo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //山东省,济南市,3000
        //山东省,青岛市,2000
        //河北省,廊坊市,1000
        DataStreamSource<String> source = env.socketTextStream("linux001", 8888);

        SingleOutputStreamOperator<Tuple3<String,String,Integer>> tpStream = source.map(line -> {
            String[] split = line.split(",");
            return Tuple3.of(split[0], split[1],Integer.parseInt(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.INT));



        // project 的功能, 是将tuple中的需要的数据字段, 返回的数据类型为Tuple
        SingleOutputStreamOperator<Tuple> project = tpStream.project(2, 0);
        // project 可以使用map方法进行代替

        project.print();

        env.execute();
    }





}
